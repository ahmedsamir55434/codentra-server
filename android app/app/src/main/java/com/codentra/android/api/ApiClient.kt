package com.codentra.android.api

import com.codentra.android.BuildConfig
import com.codentra.android.util.SessionStore
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory

object ApiClient {
    fun create(sessionStore: SessionStore): ApiService {
        val auth = Interceptor { chain ->
            val original = chain.request()
            val token = sessionStore.token()
            val req = if (!token.isNullOrBlank()) {
                original.newBuilder().header("Authorization", "Bearer $token").build()
            } else original
            chain.proceed(req)
        }

        val logger = HttpLoggingInterceptor().apply { level = HttpLoggingInterceptor.Level.BASIC }
        val client = OkHttpClient.Builder().addInterceptor(auth).addInterceptor(logger).build()

        return Retrofit.Builder()
            .baseUrl(BuildConfig.API_BASE_URL)
            .client(client)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
            .create(ApiService::class.java)
    }
}
