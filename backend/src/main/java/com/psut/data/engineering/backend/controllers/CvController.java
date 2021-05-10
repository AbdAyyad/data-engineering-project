package com.psut.data.engineering.backend.controllers;

import com.psut.data.engineering.backend.services.SubmitService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Collections;


@RestController
public class CvController {
    @Autowired
    private SubmitService submissions;

    @CrossOrigin(origins = "*")
    @PostMapping(value = "/upload")
    public ResponseEntity<?> upload(@RequestParam(value = "file") MultipartFile file) throws IOException {
        submissions.submit(file.getInputStream());
        return ResponseEntity.ok(Collections.singletonMap("status", "ok"));
    }
}
