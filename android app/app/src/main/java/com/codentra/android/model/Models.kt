package com.codentra.android.model

data class LoginRequest(
    val email: String,
    val password: String
)

data class UserDto(
    val id: String,
    val name: String?,
    val email: String?,
    val role: String?
)

data class AuthResponse(
    val token: String,
    val user: UserDto
)

data class ProjectsResponse(
    val projects: List<Project>
)

data class Project(
    val id: String,
    val title: String?,
    val description: String?,
    val price: Double?
)
