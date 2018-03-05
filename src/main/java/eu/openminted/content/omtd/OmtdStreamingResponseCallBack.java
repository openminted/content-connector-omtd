package eu.openminted.content.omtd;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.io.PipedOutputStream;

public class OmtdStreamingResponseCallBack extends StreamingResponseCallback {

    private static Logger log = Logger.getLogger(OmtdStreamingResponseCallBack.class.getName());
    private PipedOutputStream outputStream;
    private String outputField;

    public OmtdStreamingResponseCallBack(PipedOutputStream out, String field) {
        outputStream = out;
        outputField = field;
    }

    @Override
    public void streamSolrDocument(SolrDocument solrDocument) {
        try {
            String xml = solrDocument.getFieldValue(outputField).toString()
                    .replaceAll("[\\[\\]]", "");
            outputStream.write(xml.getBytes());
            outputStream.flush();
        } catch (IOException e) {
            try {
                outputStream.close();
            } catch (IOException e1) {
                log.error("OmtdStreamingResponseCallback.streamSolrDocument Inner exception!", e1);
            }
        }
    }

    @Override
    public void streamDocListInfo(long l, long l1, Float aFloat) {

    }
}
