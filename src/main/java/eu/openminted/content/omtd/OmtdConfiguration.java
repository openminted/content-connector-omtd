package eu.openminted.content.omtd;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan({"eu.openminted.content.omtd", "eu.openminted.content.connector.utils.converters"})
public class OmtdConfiguration {

}
