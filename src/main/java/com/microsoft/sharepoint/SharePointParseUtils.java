package com.microsoft.sharepoint;

import com.acl.AclInheritanceType;
import com.acl.AclType;
import com.file.ClaFilePropertiesDto;
import com.file.DiffType;
import com.file.ServerResourceDto;
import com.file.ServerResourceType;
import com.media.MediaChangeLogDto;
import com.microsoft.MSAppInfo;
import com.microsoft.MSItemKey;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.middleware.share.*;
import com.middleware.share.queryoptions.IQueryOption;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jdom2.*;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.XMLOutputter;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SharePointParseUtils {

    private static final Logger logger = LoggerFactory.getLogger(SharePointParseUtils.class);

    private static final String SITE_MARK_PREFIX = "{";
    private static final String SITE_MARK_SUFFIX = "}";

    static final String SITE_DELIMITER = "$";
    static final String BASE_PATH_COMPLETION_DELIMITER = "~";
    static final String LIST_ITEM_ID_SEPARATOR = "/";

    private static final Pattern SITE_EXTRACT_REGEX = Pattern.compile(SITE_MARK_PREFIX.replace("{", "\\{") + ".*" + SITE_MARK_SUFFIX);

    private static final String EVERYONE_USER = "c:0(.s|true";
    private static final String EVERYONE_TEXT = "\\Everyone";


    private final static ObjectMapper mapper;

    private static final List<Namespace> DEFAULT_NAMESPACE_LIST;

    private static final Namespace DEFAULT_NAMESPACE;

    private static boolean encodeSitesInUrl = true;

    static {
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES,true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Namespace defaultNameSpace = Namespace.getNamespace("a", "http://www.w3.org/2005/Atom");
        Namespace dataServicesNameSpace = Namespace.getNamespace("d", "http://schemas.microsoft.com/ado/2007/08/dataservices");
        Namespace metadataNameSpace = Namespace.getNamespace("m", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata");

        DEFAULT_NAMESPACE_LIST = Lists.newArrayList(defaultNameSpace, dataServicesNameSpace, metadataNameSpace);
        DEFAULT_NAMESPACE = DEFAULT_NAMESPACE_LIST.get(0);
    }

    public static String extractMainId(InputStream content) {
        String s;
        try {
            s = IOUtils.toString(content, StandardCharsets.UTF_8);
            logger.debug("received xml: {}", s);
            //<id>Web/Lists(guid'f1f04276-593b-454b-8ee1-006f83af18d3')/Items(6)</id>
            String mainId = StringUtils.substringBetween(s, "<d:ID ", "</d:ID>");
            if (mainId == null) {
                throw new RuntimeException("Failed to fetch folder id from response: " + s);
            }
            mainId = mainId.substring(mainId.indexOf(">") + 1);
            return mainId;
        } catch (IOException e) {
            logger.error("Failed to read input stream to extact SharePoint id", e);
            throw new RuntimeException("Failed to read input stream to extract SharePoint id");
        }
    }

    public static SharePointListItem parseSharePointListItem(InputStream content) throws Exception {
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(new InputStreamReader(content, StandardCharsets.UTF_8));
        try {
            Element element = document.getRootElement();
            return parseSharePointListItem(element);
        } catch (RuntimeException e) {
            String documentString = new XMLOutputter().outputString(document);
            logger.error("Failed to parse the resulting XML: {}", documentString, e);
            throw e;
        }
    }

    public static String extractToken(String body) throws Exception {
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(new StringReader(body));
        Collection<Namespace> nameSpaces = Lists.newArrayList(Namespace.getNamespace("wsse","http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"));
        return extractXpath(".//wsse:BinarySecurityToken", document.getRootElement(), nameSpaces);
    }

    public static SharePointListItemPage parseSharePointListItems(InputStream content) throws Exception {
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(content);
        try {
            ArrayList<SharePointListItem> items = Lists.newArrayList();
            Element rootElement = document.getRootElement();
            //Our entry elements are directly under the root element
            for (Element element : rootElement.getChildren("entry", DEFAULT_NAMESPACE)) {
                SharePointListItem sharePointListItem = parseSharePointListItem(element);
                items.add(sharePointListItem);

            }
            SharePointListItemPage result = new SharePointListItemPage(items);
            String href = extractAttributeXpath("./a:link[@rel=\"next\"]/@href", rootElement, DEFAULT_NAMESPACE_LIST);
            result.setNextUrl(href);
            return result;
        } catch (RuntimeException e) {
            String documentString = new XMLOutputter().outputString(document);
            logger.error("Failed to parse the resulting XML: {}", documentString, e);
            throw e;
        }
    }

    private static SharePointListItem parseSharePointListItem(Element element) {
        SharePointListItem sharePointListItem = new SharePointListItem();
        //Extract fileRef from d:fileref
        String fileRef = extractXpath(".//d:FileRef", element);
        sharePointListItem.setFileRef(fileRef);

        String id = extractXpath("./a:content/m:properties/d:Id", element);
        sharePointListItem.setId(id);

        String uniquePermissions = extractXpath("./a:content/m:properties/d:HasUniqueRoleAssignments", element);
        if (uniquePermissions != null) {
            sharePointListItem.setListItemHavingUniqueAcls(Boolean.valueOf(uniquePermissions));
        }

        String fileSystemObjectType = extractXpath("./a:content/m:properties/d:FileSystemObjectType", element);
        if (fileSystemObjectType != null) {
            Integer fileSystemObjectTypeInt = Integer.valueOf(fileSystemObjectType);
            sharePointListItem.setFileSystemObjectType(FileSystemObjectType.values()[fileSystemObjectTypeInt]);
        }

        String authorId = extractXpath("./a:content/m:properties/d:Authorid", element);
        String modified = extractXpath("./a:content/m:properties/d:Modified", element);
        String created = extractXpath("./a:content/m:properties/d:Created", element);
        sharePointListItem.setAuthorId(authorId);
        sharePointListItem.setModified(modified);
        sharePointListItem.setCreated(created);

        String loginName = getResolvedUsername(extractXpath(".//d:LoginName", element));
        String length = extractXpath(".//d:Length", element);
        if (length != null) {
            sharePointListItem.setSize(Long.valueOf(length));
        }
        else if (FileSystemObjectType.FILE.equals(sharePointListItem.getFileSystemObjectType())) {
            logger.warn("Failed to extract length from SharePoint list item (file)");
        }
        sharePointListItem.setLoginName(loginName);
        return sharePointListItem;
    }

    public static List<SharePointExtendedFolder> parseFolders(InputStream content) throws Exception {
        logger.trace("Parsing folders");
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(content);
        Element rootElement = document.getRootElement();
        List<SharePointExtendedFolder> result = new ArrayList<>();
        for (Element element : rootElement.getChildren("entry", DEFAULT_NAMESPACE)) {
            SharePointExtendedFolder sharePointExtendedFolder = new SharePointExtendedFolder();
            result.add(sharePointExtendedFolder);
            String name = extractXpath("./a:content/m:properties/d:Name", element);
            sharePointExtendedFolder.setName(name);
            logger.trace("Parse folder {}", name);
            String serverRelativeUrl = extractXpath("./a:content/m:properties/d:ServerRelativeUrl", element);
            sharePointExtendedFolder.setServerRelativeUrl(serverRelativeUrl);

            String itemCount = extractXpath("./a:content/m:properties/d:ItemCount", element);
            if (itemCount != null) {
                sharePointExtendedFolder.setItemCount(Integer.valueOf(itemCount));
            }

            String folderItemCount = extractXpath(".//d:vti_x005f_folderItemcount", element);
            if (folderItemCount != null) {
                sharePointExtendedFolder.setFolderItemCount(Integer.valueOf(folderItemCount));
            }

            String listTitle = extractXpath(".//d:vti_x005f_listtitle", element);
            sharePointExtendedFolder.setListTitle(listTitle);
        }
        return result;
    }

    /**
     *
     * @param content content input stream
     * @return single folder
     */
    public static SharePointExtendedFolder parseFolderProperties(InputStream content) throws Exception {
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(content);
        Element rootElement = document.getRootElement();
        String listName = extractXpath("//d:vti_x005f_listname", rootElement);
        String listTitle = extractXpath("//d:vti_x005f_listtitle", rootElement);
        SharePointExtendedFolder sharePointExtendedFolder = new SharePointExtendedFolder();
        Optional.ofNullable(extractXpath(".//d:TimeCreated", rootElement, DEFAULT_NAMESPACE_LIST))
                .map(val -> getTimeInMillis(val, true))
                .ifPresent(sharePointExtendedFolder::setCreationTime);

        Optional.ofNullable(extractXpath(".//d:TimeLastModified", rootElement, DEFAULT_NAMESPACE_LIST))
                .map(val -> getTimeInMillis(val, true))
                .ifPresent(sharePointExtendedFolder::setLastModifiedTime);

        if (listName != null && listName.length() > 2) {
            //Need to remove {} from {4EAACB86-2605-4038-8C76-52C4606E3B20}
            sharePointExtendedFolder.setListId(removeCurls(listName));
        }
        sharePointExtendedFolder.setListTitle(listTitle);
        return sharePointExtendedFolder;
    }

    private static String removeCurls(String listName) {
        return listName.substring(1,listName.length()-1);
    }

    public static List<SharePointRoleAssignment> parseRoleAssignments(InputStream input) throws Exception {
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(input);
        Namespace defaultNameSpace = Namespace.getNamespace("a", "http://www.w3.org/2005/Atom");
        Namespace dataServicesNameSpace = Namespace.getNamespace("d", "http://schemas.microsoft.com/ado/2007/08/dataservices");
        ArrayList<SharePointRoleAssignment> result = Lists.newArrayList();

        Element rootElement = document.getRootElement();
        //Our entry elements are directly under the root element
        XPathFactory xpfac = XPathFactory.instance();

        int iUser = 0;
        for (Element element : rootElement.getChildren("entry", defaultNameSpace)) {
            SharePointRoleAssignment currentRoleAssignement = new SharePointRoleAssignment();
            result.add(currentRoleAssignement);
            XPathExpression xp = xpfac.compile("./a:link[@title='Member']//d:LoginName", Filters.element(), null, defaultNameSpace, dataServicesNameSpace);
            Object resultElement = xp.evaluateFirst(element);
            if (resultElement != null) {
                String userName = ((Element) resultElement).getText();
                if (userName != null) { //i:0#.w|docauthority\testsharepoint StringUtils.substringAfter(userName,"|")
                    userName = getResolvedUsername(userName);
                    currentRoleAssignement.setLoginName(userName);

                    logger.info("Extract user {}: {}", iUser, userName);
                }
            }
            //Now extract role type
            xp = xpfac.compile("./a:link[@title='RoleDefinitionBindings']//d:RoleTypeKind", Filters.element(), null, defaultNameSpace, dataServicesNameSpace);
            resultElement = xp.evaluateFirst(element);
            if (resultElement != null) {
                String roleType = ((Element) resultElement).getText();
                logger.info("Extract roleType {}: {}", iUser, roleType);
                currentRoleAssignement.setAclType(convertSharePointRoleToAclType(roleType));
            }
            iUser++;
        }
        return result;
    }

    private static String getResolvedUsername(@NotNull String userName) {
        if (EVERYONE_USER.equalsIgnoreCase(userName)) {
            logger.trace("Detected user '{}'. Setting as {}", EVERYONE_USER, EVERYONE_TEXT);
            userName = EVERYONE_TEXT;
        } else {
            userName = convertLoginName(userName);
        }
        return userName;
    }

    private static String extractXpath(String expression, Element element) {
        return extractXpath(expression, element, DEFAULT_NAMESPACE_LIST);
    }

    private static String extractXpath(String expression, Element element, Collection<Namespace> namespaces) {
        XPathFactory xpfac = XPathFactory.instance();
        XPathExpression xp = xpfac.compile(expression, Filters.element(), null, namespaces);
        Object resultElement = xp.evaluateFirst(element);
        if (resultElement != null) {
            return ((Element) resultElement).getText();
        }
        return null;
    }

    @SuppressWarnings("SameParameterValue")
    private static String extractAttributeXpath(String expression, Element element, Collection<Namespace> namespaces) {
        XPathFactory xpfac = XPathFactory.instance();
        XPathExpression xp = xpfac.compile(expression, Filters.attribute(), null, namespaces);
        Object resultElement = xp.evaluateFirst(element);
        if (resultElement != null) {
            return  ((Attribute) resultElement).getValue();
        }
        return null;
    }

    //    http://ec2-35-165-166-205.us-west-2.compute.amazonaws.com:8080/sanuk/ChiefDataOffice/_api/web/roledefinitions
    private static AclType convertSharePointRoleToAclType(String text) {
        switch (text) {
            case "0"://Special permissions - usually write inclusive, Excel Services Viewers - 	View Only
            case "2"://Visitors, Read
                return AclType.READ_TYPE;
            case "1"://Limited Access or View Only
                return AclType.DENY_READ_TYPE;
            default:
                return AclType.WRITE_TYPE;
        }
    }

    @SuppressWarnings("unused")
    public static String encodeToInternalName(String toEncode) {
        //TODO - make more robust
        try {
            String encodedUrl = URLEncoder.encode(toEncode, "UTF8");
            encodedUrl = encodedUrl.replace("+", "_x0020_");
            return encodedUrl.replaceAll("%(..)", "_x00$1_");
        } catch (UnsupportedEncodingException e) {
            logger.error("Failed to encode string {} for sharepoint", toEncode, e);
        }
        return null;
    }

    static String parseInternalName(String name) {
        if (name == null || name.length() == 0) {
            return name;
        }
        if (!name.contains("_x")) {
            return name;
        }

        try {
            StringBuilder stringBuilder = new StringBuilder();
            char[] chars = name.toCharArray();
            int i = 0;
            while (i < chars.length) {
                char aChar = chars[i];
                if (aChar == '_' && i + 6 < chars.length) {
                    aChar = chars[i + 1]; //X
                    if (aChar != 'x') {
                        stringBuilder.append("_");
                        i++;
                        continue;
                    } else {
                        char[] numbers = new char[4];
                        numbers[0] = chars[i + 2];
                        numbers[1] = chars[i + 3];
                        numbers[2] = chars[i + 4];
                        numbers[3] = chars[i + 5];
                        int num = Integer.valueOf(new String(numbers), 16);
                        char result = (char) num;
                        stringBuilder.append(result);
                        i += 7;
                        continue;
                    }
                }
                stringBuilder.append(aChar);
                i++;
            }
            return stringBuilder.toString();
        } catch (RuntimeException e) {
            logger.error("Failed to parse sharepoint string: ", name, e);
            return name;
        }
    }

    @SuppressWarnings("unused")
    public static String convertMainIdToMediaItemId(String mainId) {
        String[] split = StringUtils.split(mainId, LIST_ITEM_ID_SEPARATOR);
        if (split.length != 3) {
            throw new RuntimeException("Illegal sharePoint id: " + mainId);
        }
        // Web, Lists..., Items
        String listId = StringUtils.substringBetween(split[1], "'");
        String itemId = StringUtils.substringBetween(split[2], "(", ")");
        return listId + LIST_ITEM_ID_SEPARATOR + itemId;
    }

    public static String encodeUrlWithSlash(String path) {
        String s = Util.encodeUrl(path);
        return s.replace("%2f", "/");
    }

    public static String createQueryOptionsUrl(List<IQueryOption> queryOptions) {
        if (queryOptions != null && queryOptions.size() != 0) {
            StringBuilder result = new StringBuilder("?");

            for (int var2 = 0; var2 < queryOptions.size(); ++var2) {
                if (queryOptions.get(var2) != null) {
                    result.append(queryOptions.get(var2).toString());
                }

                if (var2 < queryOptions.size() - 1) {
                    result.append("&");
                }
            }

            return result.toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    public static List<MediaChangeLogDto> convertToMediaChangeLogDtos(String listId, List<Change> changes) {
        List<MediaChangeLogDto> result = new ArrayList<>();
        for (Change change : changes) {
            result.add(convertToMediaChangeLogDto(listId, change));
        }

        return result;
    }


    private static MediaChangeLogDto convertToMediaChangeLogDto(String listId, Change change) {
        int itemId = change instanceof ChangeItem ? ((ChangeItem) change).getItemId() : -1;
        MediaChangeLogDto mediaChangeLogDto = new MediaChangeLogDto(listId + LIST_ITEM_ID_SEPARATOR + itemId, convertToDiffType(change.getType()));
        mediaChangeLogDto.setChangeLogPosition(convertSharePointChangeToString(change.getToken()));

        return mediaChangeLogDto;
    }

    public static DiffType convertToDiffType(ChangeType type) {
        switch (type) {
            case ADD:
            case MOVE_INTO:
                return DiffType.NEW;
            case DELETE_OBJECT:
            case MOVE_AWAY:
                return DiffType.DELETED;
            case RENAME:
                return DiffType.RENAMED;
            case RESTORE:
                return DiffType.UNDELETED;
            case ROLE_ADD:
            case ROLE_DELETE:
            case ROLE_UPDATE:
            case ASSIGNMENT_ADD:
            case ASSIGNMENT_DELETE:
            case MEMBER_ADD:
            case MEMBER_DELETE:
            case SYSTEM_UPDATE:
            case SCOPE_ADD:
            case SCOPE_DELETE:
                return DiffType.ACL_UPDATED;
            case UPDATE:
            default:
                return DiffType.CONTENT_UPDATED;
        }
    }

    /**
     * @see <a href='https://msdn.microsoft.com/en-us/library/microsoft.sharepoint.client.changetype.aspx'>MSDN doc</a>
     * @see SharePointParseUtils#convertToDiffType
     * @param changeType changeType code
     * @return Difference type
     */
    private static DiffType convertToDiffType(int changeType) {
        return convertToDiffType(ChangeType.values()[changeType - 1]);
    }

    static String applyBasePathCompletionToMediaItemId(String pathCompletion, String mediaItemId) {
        pathCompletion = normalizePath(pathCompletion).substring(1);
        return mediaItemId + BASE_PATH_COMPLETION_DELIMITER + pathCompletion.toLowerCase();
    }

    public static String calculateMediaItemId(String subSite, String mediaItemId) {
        if (mediaItemId.contains(SITE_DELIMITER)) {
            mediaItemId = mediaItemId.substring(0, mediaItemId.indexOf(SITE_DELIMITER));
        }

        String[] parts = mediaItemId.split(LIST_ITEM_ID_SEPARATOR);
        String itemId = parts.length > 1 ? parts[1] : null;
        return calculateMediaItemId(subSite, parts[0], itemId);
    }

    public static String calculateMediaItemId(String subSite, String listId, String listItemId) {
        String itemId = Optional.ofNullable(listItemId)
                .map(val -> LIST_ITEM_ID_SEPARATOR + val)
                .orElse("");

        String subsite = Optional.ofNullable(subSite)
                .map(val -> SITE_DELIMITER + val)
                .orElse("");
        return (listId + itemId + subsite)
                .toUpperCase();
    }

    static SharePointChangeTokenDto convertToSharePointChangeToken(String changeTokenStartJson) {
        try {
            return mapper.readValue(changeTokenStartJson, SharePointChangeTokenDto.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read illegal SharePoint changeToken string " + changeTokenStartJson, e);
        }
    }

    static String convertSharePointChangeToString(ChangeToken token) {
        if (token == null) {
            return null;
        }
        SharePointChangeTokenDto changeTokenDto = new SharePointChangeTokenDto(token);
        try {
            return mapper.writeValueAsString(changeTokenDto);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize change token to DTO:" + token, e);
        }

    }

    public static String convertLoginName(String loginName) {
        if (!Optional.ofNullable(loginName).orElse("").contains("|")) {
            return loginName;
        }
        return StringUtils.substringAfterLast(loginName, "|");
    }

    private static ClaFilePropertiesDto extractFilePropertiesFromEntrElement_ListMethod(Element entry) {
        final ClaFilePropertiesDto fileProp = ClaFilePropertiesDto.create();
        String name = null;
        try {
            name = extractXpath(".//d:ServerRelativeUrl", entry);
            long fileSize = Optional.ofNullable(extractXpath(".//d:Length", entry))
                    .map(Long::valueOf)
                    .orElseGet(() -> {
                        logger.info("Failed to obtain file-size. Setting as 0 entry {}", entry);
                        return 0L;
                    });

            String itemId = extractXpath(".//d:ID", entry);
            fileProp.setFolder(false);
            fileProp.setFileName(name);
            fileProp.setFileSize(fileSize);
            fileProp.setMediaItemId(itemId);

            Optional.ofNullable(extractXpath(".//d:TimeCreated", entry))
                    .map(val -> getTimeInMillis(val, true))
                    .ifPresent(fileProp::setCreationTimeMilli);

            Optional.ofNullable(extractXpath(".//d:TimeLastModified", entry))
                    .map(val -> getTimeInMillis(val, true))
                    .ifPresent(fileProp::setModTimeMilli);

            fileProp.setFolder(false);
        } catch (Exception e) {
            logger.error("Failed to parse file entry - resolved file name {} entry {}", name, entry, e);
            fileProp.setFileName(name);
        }
        return fileProp;
    }

    private static ClaFilePropertiesDto extractFilePropertiesFromEntryElement(Element entry) {
        final ClaFilePropertiesDto fileProp = ClaFilePropertiesDto.create();
        String name = null;
        try {
            name = extractXpath(".//d:FileRef", entry);
            final String tempName = name;
            long fileSize = Optional.ofNullable(extractXpath(".//d:vti_x005f_filesize", entry))
                    .map(Long::valueOf)
                    .orElseGet(() -> {
                        logger.info("Failed to obtain file-size. Setting as 0 file={} entry={}", tempName, entry);
                        return 0L;
                    });

            String itemId = extractXpath(".//d:ID", entry, DEFAULT_NAMESPACE_LIST);
            String uniqueAcl = extractXpath(".//d:HasUniqueRoleAssignments", entry, DEFAULT_NAMESPACE_LIST);

            fileProp.setFileName(name);
            fileProp.setFileSize(fileSize);

            fileProp.setMediaItemId(itemId);

            String ownerWithDomain = extractXpath(".//d:vti_x005f_author", entry, DEFAULT_NAMESPACE_LIST);
            if (!Strings.isNullOrEmpty(ownerWithDomain)) {
                fileProp.setOwnerName(convertLoginName(ownerWithDomain));
            }

            Optional.ofNullable(extractXpath(".//d:vti_x005f_timecreated", entry))
                    .map(val -> getTimeInMillis(val, false))
                    .ifPresent(fileProp::setCreationTimeMilli);

            Optional.ofNullable(extractXpath(".//d:vti_x005f_timelastmodified", entry))
                    .map(val -> getTimeInMillis(val, false))
                    .ifPresent(fileProp::setModTimeMilli);

            fileProp.setFolder(false);
            if (uniqueAcl != null) {
                AclInheritanceType aclType = Boolean.valueOf(uniqueAcl) ? AclInheritanceType.NONE : AclInheritanceType.FOLDER;
                fileProp.setAclInheritanceType(aclType);
            }
        } catch (Exception e) {
            logger.error("Failed to parse file entry - resolved file name {} entry {}", name, entry, e);
            fileProp.setFileName(name);
        }
        return fileProp;
    }

    private static ClaFilePropertiesDto extractFilePropertiesFromEntryElement2013(Element entry) {
        final ClaFilePropertiesDto fileProp = ClaFilePropertiesDto.create();
        String name = null;
        try {
            name = extractXpath(".//d:ServerRelativeUrl", entry);
            long fileSize = Optional.ofNullable(extractXpath(".//d:Length", entry))
                    .map(Long::valueOf)
                    .orElse(0L);

            fileProp.setMediaItemId(extractXpath(".//d:ID", entry));

            fileProp.setFileName(name);
            fileProp.setFileSize(fileSize);
            Optional.ofNullable(extractXpath(".//d:TimeCreated", entry))
                    .map(val -> getTimeInMillis(val, true))
                    .ifPresent(fileProp::setCreationTimeMilli);

            Optional.ofNullable(extractXpath(".//d:TimeLastModified", entry))
                    .map(val -> getTimeInMillis(val, true))
                    .ifPresent(fileProp::setModTimeMilli);

            fileProp.setOwnerName(extractXpath(".//d:LoginName", entry));
            fileProp.setFolder(false);
        } catch (Exception e) {
            logger.error("Failed to parse file entry - resolved file name {} entry {}", name, entry, e);
            fileProp.setFileName(name);
        }
        return fileProp;
    }

    public static ClaFilePropertiesDto convertFileItemListToFileList_ListMethod(InputStream is) throws JDOMException, IOException {
        return convertFileItemFileProperty(is, SharePointParseUtils::extractFilePropertiesFromEntrElement_ListMethod);
    }

    public static List<ClaFilePropertiesDto> convertFileItemListToFileList(InputStream is) throws JDOMException, IOException {
        return convertFileItemListToFileList(is, SharePointParseUtils::extractFilePropertiesFromEntryElement);
    }

    public static List<ClaFilePropertiesDto> convertFileItemListToFileList2013(InputStream is) throws JDOMException, IOException {
        return convertFileItemListToFileList(is, SharePointParseUtils::extractFilePropertiesFromEntryElement2013);
    }

    private static List<ClaFilePropertiesDto> convertFileItemListToFileList(InputStream is, Function<Element, ClaFilePropertiesDto> dataExtractor) throws JDOMException, IOException {
        List<ClaFilePropertiesDto> files = Lists.newArrayList();
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(new InputStreamReader(is, StandardCharsets.UTF_8));
        Element rootElement = document.getRootElement();
        for (Element element : rootElement.getChildren("entry", DEFAULT_NAMESPACE)) {
            files.add(dataExtractor.apply(element));
        }

        return files;
    }

    public static ClaFilePropertiesDto convertFileItemFileProperty(InputStream inputStream) throws JDOMException, IOException {
        return convertFileItemFileProperty(inputStream, SharePointParseUtils::extractFilePropertiesFromEntryElement);
    }

    public static ClaFilePropertiesDto extractFileMediaItemId(InputStream inputStream) throws JDOMException, IOException {
        Function<Element, ClaFilePropertiesDto> idExtractor = element -> {
            String listId = removeCurls(extractXpath(".//d:vti_x005f_listid", element));
            String itemId = extractXpath(".//d:Id", element);
            String ownerName = extractXpath(".//d:vti_x005f_author", element, DEFAULT_NAMESPACE_LIST);

            ClaFilePropertiesDto dto = ClaFilePropertiesDto.create();
            dto.setOwnerName(ownerName);
            dto.setMediaItemId(calculateMediaItemId(null, listId, itemId));
            return dto;
        };
        return convertFileItemFileProperty(inputStream, idExtractor);
    }

    public static ClaFilePropertiesDto convertFileItemFileProperty2013(InputStream inputStream) throws JDOMException, IOException {
        return convertFileItemFileProperty(inputStream, SharePointParseUtils::extractFilePropertiesFromEntryElement2013);
    }

    private static ClaFilePropertiesDto convertFileItemFileProperty(InputStream inputStream, Function<Element, ClaFilePropertiesDto> dataExtractor) throws JDOMException, IOException {
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        Element rootElement = document.getRootElement();
        return dataExtractor.apply(rootElement);
    }

    public static List<MediaChangeLogDto> convertToMediaChangeLog(InputStream inputStream) throws JDOMException, IOException {
        List<MediaChangeLogDto> changes = Lists.newArrayList();
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(inputStream);
        Element rootElement = document.getRootElement();
        for (Element element : rootElement.getChildren("entry", DEFAULT_NAMESPACE)) {
            String listId  = extractXpath(".//d:ListId", element);
            String itemId = extractXpath(".//d:ItemId", element);
            String changeType = extractXpath(".//d:ChangeType", element);
            String mediaItemId = (listId + LIST_ITEM_ID_SEPARATOR + itemId).toUpperCase();
            MediaChangeLogDto changeLog = new MediaChangeLogDto(mediaItemId, convertToDiffType(Integer.parseInt(changeType)));
            changes.add(changeLog);
        }

        return changes;
    }

    public static long getTimeInMillis(String time, boolean endsWithZ) {
        String pattern = "yyyy-MM-dd'T'HH:mm:ss";
        if (endsWithZ) {
            pattern += "'Z'";
        }
        return LocalDateTime.parse(time, DateTimeFormatter.ofPattern(pattern))
                .atOffset(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
    }

    public static String normalizePath(String path) {
        if (path == null) {
            logger.trace("normalizePath(null)=null");
            return StringUtils.EMPTY;
        }
        String origPath = path;
        path = removeUnneededDoubleSlashes(path).trim();
        String out = path.startsWith("http") || path.startsWith("/") ? path : ("/" + path);
        if (path.endsWith("/")) {
            out = out.replaceAll("[/]+$", "");
        }

        logger.trace("sub-site mappings: normalizePath({})={}", origPath, out);
        return out;
    }

    public static String removeUnneededDoubleSlashes(String url) {
        return url.replaceAll("(?<!:)[/]{2,}", "/");
    }

    /**
     * Case Insensitive
     */
    public static boolean pathsEquals(String pathA, String pathB) {
        pathA = Optional.ofNullable(pathA)
                .map(SharePointParseUtils::normalizePath)
                .orElse(StringUtils.EMPTY);
        pathB = Optional.ofNullable(pathB)
                .map(SharePointParseUtils::normalizePath)
                .orElse(StringUtils.EMPTY);
        return pathA.equalsIgnoreCase(pathB);

    }

    public static List<ServerResourceDto> extractSubSites(InputStream inputStream) throws JDOMException, IOException {
        Function<Element, ServerResourceDto> extractor = elem -> {
            String relativeUrl = extractXpath(".//d:ServerRelativeUrl", elem);
            String title = extractXpath(".//d:Title", elem);
            ServerResourceDto dto = new ServerResourceDto(relativeUrl, title);
            dto.setType(ServerResourceType.SITE);
            return dto;
        };

        return parseXml(inputStream, extractor);
    }

    private static <T> List<T> parseXml(InputStream inputStream, Function<Element, T> dataExtractor) throws JDOMException, IOException {
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        Element rootElement = document.getRootElement();

        final List<T> items = Lists.newArrayList();
        for (Element element : rootElement.getChildren("entry", DEFAULT_NAMESPACE)) {
            items.add(dataExtractor.apply(element));
        }
        return items;
    }

    public static List<ServerResourceDto> extractSubSiteFolders(InputStream inputStream) throws JDOMException, IOException {
        Function<Element, ServerResourceDto> extractor = elem -> {
            ServerResourceDto folder = new ServerResourceDto();
            String name = null;
            try {
                name = extractXpath(".//d:Name", elem);
                folder.setName(name);

                folder.setFullName(extractXpath(".//d:ServerRelativeUrl", elem));
                folder.setType(ServerResourceType.FOLDER);
            } catch (Exception e) {
                logger.error("Failed to parse sub-site folder, resolved name: {} elem {}", name, elem, e);
                Optional.ofNullable(name)
                        .ifPresent(folder::setName);
            }
            return folder;
        };
        return parseXml(inputStream, extractor);
    }

    //    http://dom:8080/site/{sub1}/{sub2}/folder/file ==> http://dom:8080/site/sub1/sub2/folder/file, sub1/sub2
    public static MSItemKey splitPathAndSubsite(String path) {
        Matcher matcher = SITE_EXTRACT_REGEX.matcher(path);
        String subSite = null;
        String origPath = path;
        if (matcher.find()) {
            subSite = matcher.group();
            subSite = subSite.substring(SITE_MARK_PREFIX.length(), subSite.length() - SITE_MARK_SUFFIX.length());
            subSite = subSite.replaceAll("}/\\{", "/");
            path = path.substring(0, matcher.start()) + subSite + path.substring(matcher.end());
        }

        logger.trace("sub-site mappings: splitPathAndSubsite({})={},{}", origPath, path, subSite);
        return MSItemKey.path(subSite, path);
    }

    public static String encodeSubSiteNameIfNeeded(String subSite) {
        if (encodeSitesInUrl && subSite != null) {
            String[] parts = subSite.split("/");
            if (parts != null) {
                String subSiteFixed = (subSite.startsWith("/") ? "/" : "");
                for (String part : parts) {
                    if (!part.isEmpty()) {
                        part = Util.encodeUrl(part);
                        subSiteFixed += (part + "/");
                    }
                }
                if (!subSite.endsWith("/")) {
                    subSiteFixed = subSiteFixed.substring(0, subSiteFixed.length()-1);
                }
                subSite = subSiteFixed;
            }
        }
        return subSite;
    }

    public static String applySiteMark(String path, String site) {
        if (site == null) {
            logger.trace("sub-site mappings: applySiteMark({},)={}", path, path);
            return path;
        } else if (path == null) {
            logger.trace("sub-site mappings: applySiteMark(,{})={}", site, site);
            return site;
        }
        String origPath = path;

        Matcher matcher = SITE_EXTRACT_REGEX.matcher(path);
        if (matcher.find()) {
            path = splitPathAndSubsite(path).getPath();
        }

        path = normalizePath(path);
        site = normalizePath(site);

        int pos = path.toLowerCase().indexOf(site.toLowerCase());
        if (pos == -1) {
            pos = path.length();
            path = path + site;
        }

        String res = path.substring(0, pos)
                + "/"
                + SITE_MARK_PREFIX
                + site.substring(1).replaceAll("/", "}/{")
                + SITE_MARK_SUFFIX
                + path.substring(pos + site.length());
        logger.trace("sub-site mappings: applySiteMark({},{})={}", origPath, site, res);
        return res;
    }


    public static MSItemKey splitMediaItemIdAndSite(String mediaItemId) {
        String[] basePathCompletionAndMediaId = mediaItemId.split(BASE_PATH_COMPLETION_DELIMITER);
        String basePathCompletion = basePathCompletionAndMediaId.length > 1 ? basePathCompletionAndMediaId[1] : null;

        String[] siteAndMediaId = basePathCompletionAndMediaId[0].split("\\" + SITE_DELIMITER);
        String site = siteAndMediaId.length > 1 ? siteAndMediaId[1] : null;

        String[] mediaIdSplit = siteAndMediaId[0].split(LIST_ITEM_ID_SEPARATOR);
        String itemId = mediaIdSplit.length > 1 ? mediaIdSplit[1] : null;

        MSItemKey key = MSItemKey.listItem(basePathCompletion, site, mediaIdSplit[0], itemId);
        logger.trace("sub-site mappings: splitMediaItemIdAndSite({})={}", mediaItemId, key);
        return key;
    }

    public static String parseUserAgent(MSAppInfo appInfo) {
        return Optional.ofNullable(appInfo)
                .filter(app -> Objects.nonNull(app.company) && Objects.nonNull(app.appName) && Objects.nonNull(app.version))
                .map(app -> "NONISV|" + app.company + "|" + app.appName + "/" + app.version)
                .orElse(null);
    }

    public static void setEncodeSitesInUrl(boolean encodeSitesInUrl) {
        SharePointParseUtils.encodeSitesInUrl = encodeSitesInUrl;
    }
}
