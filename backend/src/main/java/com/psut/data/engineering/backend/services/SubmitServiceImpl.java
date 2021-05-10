package com.psut.data.engineering.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import java.io.InputStream;
import java.nio.file.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


@Service
public class SubmitServiceImpl implements SubmitService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubmitServiceImpl.class);
    private ObjectMapper mapper;
    @Value("${data-engineering.server.url}")
    private String serverUrl;

    public SubmitServiceImpl() {
        createDirectoryIfNotExist();
        mapper = new ObjectMapper();
    }

    private void createDirectoryIfNotExist() {
        try {
            Path path = Paths.get("/home/spring/uploaded-cv");
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

    }

    @Override
    public void submit(InputStream stream) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            String uuid = UUID.randomUUID().toString();
            Path path = Paths.get("/home/spring/uploaded-cv");
            Path tmp = Files.createTempFile(path,"",".pdf");
            Files.copy(stream, tmp, StandardCopyOption.REPLACE_EXISTING);
            LOGGER.info("file " + tmp + " saved");

            HttpPost request = new HttpPost(serverUrl+"/api/v1/dags/cv/dagRuns");
            request.addHeader("Authorization","Basic YWlyZmxvdzphaXJmbG93");
            request.addHeader("Content-Type","application/json");
            request.addHeader("Accept","application/json");
            Map<String, Object> body = new HashMap<>();
            body.put("dag_run_id", uuid);

            String nowAsISO = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'"));

            body.put("execution_date", nowAsISO);
            Map<String, Object> conf = new HashMap<>();
            conf.put("path", tmp.toString());
            conf.put("file_id", uuid);
            body.put("conf", conf);

            request.setEntity(new StringEntity(mapper.writeValueAsString(body)));

            CloseableHttpResponse response = client.execute(request);

            LOGGER.info("airflow response status code: " + response.getStatusLine().getStatusCode());
            LOGGER.info("airflow response: " + IOUtils.toString(response.getEntity().getContent()));

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.toString());
        }
    }
}
