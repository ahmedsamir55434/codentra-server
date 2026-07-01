package com.codentra.android.api

import com.codentra.android.model.AuthResponse
import com.codentra.android.model.LoginRequest
import com.codentra.android.model.ProjectsResponse
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST

interface ApiService {
    @POST("api/auth/login")
    suspend fun login(@Body body: LoginRequest): AuthResponse

    @GET("api/projects")
    suspend fun projects(): ProjectsResponse
}
