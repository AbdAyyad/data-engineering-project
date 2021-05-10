package com.psut.data.engineering.backend.services;

import org.springframework.lang.NonNull;

import java.io.InputStream;

public interface SubmitService {

    void submit(@NonNull InputStream stream);
}
