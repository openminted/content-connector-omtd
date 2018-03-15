package eu.openminted.content.omtd;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.connector.utils.SearchExtensions;
import eu.openminted.content.connector.utils.converters.LanguageTypeConverter;
import eu.openminted.content.connector.utils.converters.PublicationTypeConverter;
import eu.openminted.content.connector.utils.converters.RightsStmtNameConverter;
import eu.openminted.content.connector.utils.faceting.OMTDFacetEnum;
import eu.openminted.content.connector.utils.faceting.OMTDFacetLabels;
import eu.openminted.content.connector.utils.language.LanguageUtils;
import eu.openminted.registry.core.domain.Facet;
import eu.openminted.registry.core.domain.Value;
import eu.openminted.registry.domain.DocumentTypeEnum;
import eu.openminted.registry.domain.PublicationTypeEnum;
import eu.openminted.registry.domain.RightsStatementEnum;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class OmtdContentConnector implements ContentConnector{

    private static Logger log = Logger.getLogger(OmtdContentConnector.class.getName());

    @org.springframework.beans.factory.annotation.Value("${solr.hosts}")
    private String hosts;

    @org.springframework.beans.factory.annotation.Value("${solr.query.output.field}")
    private String queryOutputField;

    @org.springframework.beans.factory.annotation.Value("${solr.client.type}")
    private String solrClientType;

    @org.springframework.beans.factory.annotation.Value("${content.limit:0}")
    private Integer contentLimit;

    @org.springframework.beans.factory.annotation.Value("${solr.omtd.collection}")
    private String defaultCollection;

    @org.springframework.beans.factory.annotation.Value("${solr.update.default.collection:false}")
    private boolean updateCollection;

    private int start = 0;

    private int rows = 0;

    private int limit = 0;

    @Autowired
    private OMTDFacetLabels omtdFacetLabels;

    @Autowired
    private RightsStmtNameConverter rightsStmtNameConverter;

    @Autowired
    private PublicationTypeConverter publicationTypeConverter;

    @Autowired
    private LanguageTypeConverter languageTypeConverter;

    /**
     * Search method for browsing metadata
     *
     * @param query the query as inserted in content connector service
     * @return SearchResult with metadata and facets
     */
    @Override
    public SearchResult search(Query query) throws IOException {
        Query tmpQuery = new Query(query.getKeyword(), query.getParams(), query.getFacets(), query.getFrom(), query.getTo());
        if (tmpQuery.getKeyword() == null || tmpQuery.getKeyword().isEmpty()) {
            tmpQuery.setKeyword("*:*");
        }

        buildSort(tmpQuery);
        buildFields(tmpQuery);
        buildParams(tmpQuery);

        tmpQuery.getParams().remove(OMTDFacetEnum.SOURCE.value());
        tmpQuery.getFacets().remove(OMTDFacetEnum.SOURCE.value());


        SearchResult searchResult = new SearchResult();
        try {
            SolrClient solrClient = new HttpSolrClient.Builder(hosts+defaultCollection).build();
            QueryResponse response = solrClient.query(queryBuilder(tmpQuery));
            searchResult.setPublications(new ArrayList<>());

            if (response.getResults() != null) {
                searchResult.setFrom((int) response.getResults().getStart());
                searchResult.setTo((int) response.getResults().getStart() + response.getResults().size());
                searchResult.setTotalHits((int) response.getResults().getNumFound());

//                for (SolrDocument document : response.getResults()) {
//                    String xml = document.getFieldValue(queryOutputField).toString().replaceAll("[\\[\\]]", "");
//                    xml = xml.trim();
//                    parser.parse(new InputSource(new StringReader(xml)));
//                    searchResult.getPublications().add(parser.getOMTDPublication());
//                }
            } else {
                searchResult.setFrom(query.getFrom());
                searchResult.setTo(query.getFrom());
                searchResult.setTotalHits(0);
            }

            Map<String, Facet> facets = new HashMap<>();

            if (response.getFacetFields() != null) {
                for (FacetField facetField : response.getFacetFields()) {
                    Facet facet = buildFacet(facetField);
                    if (facet != null && facet.getValues() != null && facet.getValues().size() > 0) {
                        // merge values that contain the same value.value
                        facet.setValues(SearchExtensions.mergeValues(facet));
                        if (!facets.containsKey(facet.getField())) {
                            facets.put(facet.getField(), facet);
                        } else {
                            // in case another facet with the same facet.field exists,
                            // merge the two facets and add the product to the map
                            // instead of the initial facet
                            facets.put(facet.getField(), SearchExtensions.mergeFacet(facets.get(facet.getField()), facet));
                        }
                    }
                }

                Facet facet = facets.get(OMTDFacetEnum.DOCUMENT_TYPE.value());
                if (facet != null) {
                    String abstractLabel = omtdFacetLabels.getDocumentTypeLabelFromEnum(DocumentTypeEnum.WITH_ABSTRACT_ONLY);
                    Value abstractsValue = new Value();
                    abstractsValue.setValue(abstractLabel);
                    abstractsValue.setLabel(abstractLabel);
                    int count = 0;
                    for (Value value : facet.getValues()) {
                        count += value.getCount();
                    }
                    abstractsValue.setCount(searchResult.getTotalHits() - count);
                    facet.getValues().add(abstractsValue);
                }
            }

            searchResult.setFacets(new ArrayList<>(facets.values()));

        } catch (SolrServerException e) {
          log.error("Error in content connector OMTD ",e);
        }
        ArrayList<String> tempArrayList = new ArrayList<>();
        tempArrayList.add("OMTD");
        tmpQuery.getParams().put("source", tempArrayList);
        tmpQuery.getFacets().add(OMTDFacetEnum.SOURCE.value());
        return searchResult;
    }

    @Override
    public InputStream downloadFullText(String documentId) {
        InputStream inputStream = null;
        try {
            Query query = new Query();
            query.setParams(new HashMap<>());
            // use escape characters for special symbol of ':' in metadata identifier
            documentId = documentId.replaceAll("\\:", "\\\\:");

            query.getParams().put("id", new ArrayList<>());
            query.getParams().get("id").add(documentId);
            query.setKeyword("*:*");

            try (SolrClient solrClient = new HttpSolrClient.Builder(hosts+defaultCollection).build()) {
                QueryResponse response = solrClient.query(queryBuilder(query));
                if (response.getResults() != null) {
                    for (SolrDocument document : response.getResults()) {
                        String downloadUrl;
                        try {
                            if (document.getFieldValue("distributionLocation") != null) {
                                downloadUrl = document.getFieldValue("distributionLocation").toString();
                                URL url = new URL(downloadUrl);
                                URLConnection connection = url.openConnection();
                                connection.connect();
                                String contentType = connection.getContentType();
                                if (contentType.toLowerCase().contains("html")) continue;
                                inputStream = url.openStream();
                                break;
                            }
                        } catch (IOException e) {
                            log.error("downloadFullText: Error while streaming document. Proceeding to next document if any!");
                        }
                    }
                }

            }
        } catch (MalformedURLException e) {
            log.error("downloadFullText: MalformedURLException ", e);
        } catch (IOException e) {
            log.error("downloadFullText: IOException ", e);
        } catch (Exception e) {
            log.error("downloadFullText: Exception ", e);
        }
        return inputStream;
    }

    @Override
    public InputStream fetchMetadata(Query query) {

        Query tmpQuery = new Query(query.getKeyword(), query.getParams(), query.getFacets(), query.getFrom(), query.getTo());

        if (tmpQuery.getKeyword() == null || tmpQuery.getKeyword().isEmpty()) {
            tmpQuery.setKeyword("*:*");
        }

        // Setting query rows up to 10 for improving speed between fetching and importing metadata
        // and not waiting log for metadata to load in memory prior their transport to the omtd service
        tmpQuery.setTo(10);

        final Query omtdQuery = tmpQuery;
        PipedInputStream inputStream = new PipedInputStream();
        PipedOutputStream outputStream = new PipedOutputStream();

        try {
            new Thread(() -> {
                try (SolrClient solrClient = new HttpSolrClient.Builder(hosts+defaultCollection).build()) {
                    fetchMetadata(omtdQuery, new OmtdStreamingResponseCallBack(outputStream, queryOutputField));
                    outputStream.flush();
                    outputStream.write("</omtdMetadataRecords>\n".getBytes());
                } catch (Exception e) {
                    log.info("Fetching metadata has been interrupted. See debug for details!");
                    log.info("SolrClient.fetchMetadata", e);
                } finally {
                    try {
                        outputStream.close();
                    } catch (IOException e) {
                        log.error("SolrClient.fetchMetadata", e);
                    }
                }
            }).start();

            outputStream.connect(inputStream);
            outputStream.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n".getBytes());
            outputStream.write("<omtdMetadataRecords>\n".getBytes());
        } catch (IOException e) {
            log.info("Fetching metadata has been interrupted. See debug for details!");
            log.info("SolrContentConnector.fetchMetadata", e);
            try {
                inputStream.close();
                outputStream.close();
            } catch (IOException e1) {
                log.error("SolrContentConnector.fetchMetadata Inner exception!", e1);
            }
        } catch (Exception e) {
            log.error("SolrContentConnector.fetchMetadata Generic exception!", e);
        }

        return inputStream;
    }

    @Override
    public String getSourceName() {
        return "OMTD";
    }

    public SolrQuery queryBuilder(Query query) {

        if (query.getFrom() > 0) {
            this.start = query.getFrom();
        }

        if (query.getTo() > 0) {
            this.rows = query.getTo() - this.start;
        }

        SolrQuery solrQuery = (new SolrQuery()).setStart(this.start).setRows(this.rows);

        if (query.getFacets() != null) {
            solrQuery.setFacet(true);
            solrQuery.setFacetLimit(-1);

            if (query.getFacets().size() > 0) {
                solrQuery.addFacetField(query.getFacets().toArray(new String[query.getFacets().size()]));
            }
        }

        if (query.getParams() != null) {
            for (String key : query.getParams().keySet()) {
                if (key.equalsIgnoreCase("sort")) {
                    for (String sortField : query.getParams().get("sort")) {
                        String[] sortingParameter = sortField.split(" ");
                        if (sortingParameter.length == 2) {
                            SolrQuery.ORDER order = SolrQuery.ORDER.valueOf(sortingParameter[1]);
                            solrQuery.setSort(sortingParameter[0], order);
                        } else if (sortingParameter.length == 1) {
                            solrQuery.setSort(sortingParameter[0], SolrQuery.ORDER.desc);
                        }
                    }
                } else if (key.equalsIgnoreCase("fl")) {
                    for (String field : query.getParams().get("fl")) {
                        solrQuery.addField(field);
                    }
                } else {
                    List<String> vals = query.getParams().get(key);

                    if (key.toLowerCase().contains("year") || key.toLowerCase().contains("date")) {
                        SimpleDateFormat yearFormat = new SimpleDateFormat("YYYY");
                        SimpleDateFormat queryDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                        TimeZone UTC = TimeZone.getTimeZone("UTC");
                        queryDateFormat.setTimeZone(UTC);
                        StringBuilder datetimeFieldQuery = new StringBuilder();
                        for (String val : vals) {
                            Date date;
                            String queryDate;
                            try {

                                yearFormat.parse(val);
                                val = val + "-01-01T00:00:00.000Z";
                                date = queryDateFormat.parse(val);
                                queryDate = queryDateFormat.format(date);
                                datetimeFieldQuery.append(key).append(":[").append(queryDate).append(" TO ").append(queryDate).append("+1YEAR] OR ");
                            } catch (ParseException e) {
                                try {
                                    date = queryDateFormat.parse(val);
                                    queryDate = queryDateFormat.format(date);
                                    datetimeFieldQuery.append(key).append(":[").append(queryDate).append(" TO ").append(queryDate).append("+1YEAR] OR ");
                                } catch (ParseException e1) {
                                    e1.printStackTrace();
                                }
                            }
                        }
                        datetimeFieldQuery = new StringBuilder(datetimeFieldQuery.toString().replaceAll(" OR $", ""));
                        if (!datetimeFieldQuery.toString().isEmpty())
                            solrQuery.addFilterQuery(datetimeFieldQuery.toString());
                    } else {
                        StringBuilder fieldQuery = new StringBuilder();
                        for (String val : vals) {
                            fieldQuery.append(key).append(":").append(val).append(" OR ");
                        }
                        fieldQuery = new StringBuilder(fieldQuery.toString().replaceAll(" OR $", ""));
                        if (!fieldQuery.toString().isEmpty())
                            solrQuery.addFilterQuery(fieldQuery.toString());
                    }
                }
            }
        }


        solrQuery.setQuery(query.getKeyword());

        log.info(solrQuery.toString());

        return solrQuery;
    }

    public void fetchMetadata(Query query, StreamingResponseCallback streamingResponseCallback) throws IOException {
        if (streamingResponseCallback == null) return;

        SolrQuery solrQuery = queryBuilder(query);
        String cursorMark = CursorMarkParams.CURSOR_MARK_START;
        boolean done = false;
        int count = 0;

        // In order to cursor functionality to work start should be 0
        if (solrQuery.getStart() != 0) solrQuery.setStart(0);

        try (SolrClient solrClient = new HttpSolrClient.Builder(hosts+defaultCollection).build()) {
            while (!done) {
                solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
                QueryResponse rsp = solrClient.queryAndStreamResponse(defaultCollection,
                        solrQuery,
                        streamingResponseCallback);

                String nextCursorMark = rsp.getNextCursorMark();
                count += solrQuery.getRows();

                if (cursorMark.equals(nextCursorMark) || (limit > 0 && count >= limit)) {
                    done = true;
                }
                cursorMark = nextCursorMark;
            }
        } catch (SolrServerException e) {
            log.info("Fetching metadata has been interrupted. See Debug for more information!");
            log.debug("OpenAireSolrClient.fetchMetadata", e);
        }
    }


    private void buildSort(Query query) {

        if (query.getParams() == null) query.setParams(new HashMap<>());
        if (!query.getParams().containsKey("sort")) {
            query.getParams().put("sort", new ArrayList<>());
            query.getParams().get("sort").add("id desc");
        } else {
            if (!query.getParams().get("sort").contains("id desc")
                    && !query.getParams().get("sort").contains("id asc")) {
                query.getParams().get("sort").add("id desc");
            }
        }
    }

    private void buildFields(Query query) {

        if (query.getParams() == null) query.setParams(new HashMap<>());

        if (!query.getParams().containsKey("fl")) {
            query.getParams().put("fl", new ArrayList<>());
            query.getParams().get("fl").add(queryOutputField);
        } else {
            if (!query.getParams().get("fl").contains(queryOutputField)) {
                query.getParams().get("fl").add(queryOutputField);
            }
        }
    }

    private Facet buildFacet(FacetField facetField) {

        Facet facet = null;
        String facetFieldName = facetField.getName().toLowerCase();
        facet = new Facet();
        String field =facetFieldName;
        facet.setField(field);
        OMTDFacetEnum facetEnum = OMTDFacetEnum.fromValue(field);
        facet.setLabel(omtdFacetLabels.getFacetLabelsFromEnum(facetEnum));
        List<Value> values = new ArrayList<>();
        LanguageUtils languageUtils = new LanguageUtils();

        for (FacetField.Count count : facetField.getValues()) {
            Value value = new Value();
            if (field.equalsIgnoreCase(OMTDFacetEnum.RIGHTS.value())) {
                RightsStatementEnum rightsStatementEnum = rightsStmtNameConverter.convertToOMTD(count.getName());

                if (rightsStatementEnum != null
                        && !omtdFacetLabels.getRightsStmtLabelFromEnum(rightsStatementEnum).isEmpty()) {
                    value.setValue(omtdFacetLabels.getRightsStmtLabelFromEnum(rightsStatementEnum));
                    value.setLabel(omtdFacetLabels.getRightsStmtLabelFromEnum(rightsStatementEnum));
                } else {
                    value.setValue(count.getName());
                    value.setLabel(count.getName());
                }
            } else if (field.equalsIgnoreCase(OMTDFacetEnum.PUBLICATION_TYPE.value())) {
                PublicationTypeEnum publicationTypeEnum = publicationTypeConverter.convertToOMTD(count.getName());

                if (!omtdFacetLabels.getPublicationTypeLabelFromEnum(publicationTypeEnum).isEmpty()) {
                    value.setValue(omtdFacetLabels.getPublicationTypeLabelFromEnum(publicationTypeEnum));
                    value.setLabel(omtdFacetLabels.getPublicationTypeLabelFromEnum(publicationTypeEnum));
                } else {
                    value.setValue(count.getName());
                    value.setLabel(count.getName());
                }
            } else if (field.equalsIgnoreCase(OMTDFacetEnum.DOCUMENT_LANG.value())) {
                String language = languageTypeConverter.convertCodeToLanguage(count.getName());

                if (language != null) {
                    value.setValue(language.toLowerCase());
                    value.setLabel(languageUtils.getLangCodeToName().get(value.getValue()));
                }
            } else if (field.equalsIgnoreCase(OMTDFacetEnum.PUBLICATION_YEAR.value())) {
                value.setValue(count.getName().substring(0, 4));
                value.setLabel(count.getName().substring(0, 4));

            } else if (field.equalsIgnoreCase(OMTDFacetEnum.DOCUMENT_TYPE.value())) {
                String fulltextDocumentType = omtdFacetLabels
                        .getDocumentTypeLabelFromEnum(DocumentTypeEnum.WITH_FULL_TEXT);


                for (Value value1 : values) {
                    if (value1.getValue().equalsIgnoreCase(fulltextDocumentType)) {
                        value = value1;
                        break;
                    }
                }
                value.setValue(fulltextDocumentType);
                value.setLabel(fulltextDocumentType);

            } else {
                value.setValue(count.getName());
                value.setLabel(count.getName());
            }

            value.setCount(value.getCount() + (int)count.getCount());

            if (!values.contains(value))
                values.add(value);
        }
        facet.setValues(values);

        return facet;
    }

    private void buildParams(Query query) {

        Map<String, List<String>> omtdParams = new HashMap<>();

        if (query.getParams() != null && query.getParams().size() > 0) {

            for (String key : query.getParams().keySet()) {

                if (key.equalsIgnoreCase(OMTDFacetEnum.SOURCE.value())) continue;

                else if (key.equalsIgnoreCase(OMTDFacetEnum.DOCUMENT_LANG.value())) {
                    String languageKey = key.toLowerCase();
                    omtdParams.put(languageKey, new ArrayList<>());

                    // populate params with language code Ids
                    for (String languageValue : query.getParams().get(key)) {
                        omtdParams.get(languageKey).add(languageValue.toLowerCase());
                    }
                }

                query.setParams(omtdParams);
            }
        }
    }

}
