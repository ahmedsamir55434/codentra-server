package com.codentra.android.ui

import android.content.Intent
import android.os.Bundle
import android.widget.Button
import android.widget.EditText
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.codentra.android.R
import com.codentra.android.api.ApiClient
import com.codentra.android.model.LoginRequest
import com.codentra.android.util.SessionStore
import kotlinx.coroutines.launch

class LoginActivity : AppCompatActivity() {
    private lateinit var session: SessionStore

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_login)

        session = SessionStore(this)
        if (!session.token().isNullOrBlank()) {
            startActivity(Intent(this, ProjectsActivity::class.java))
            finish()
            return
        }

        val email = findViewById<EditText>(R.id.emailInput)
        val password = findViewById<EditText>(R.id.passwordInput)
        val loginBtn = findViewById<Button>(R.id.loginBtn)

        loginBtn.setOnClickListener {
            val e = email.text.toString().trim()
            val p = password.text.toString()
            if (e.isBlank() || p.isBlank()) {
                Toast.makeText(this, "اكتب الايميل والباسورد", Toast.LENGTH_SHORT).show()
                return@setOnClickListener
            }

            loginBtn.isEnabled = false
            lifecycleScope.launch {
                try {
                    val api = ApiClient.create(session)
                    val resp = api.login(LoginRequest(e, p))
                    session.saveToken(resp.token)
                    startActivity(Intent(this@LoginActivity, ProjectsActivity::class.java))
                    finish()
                } catch (ex: Exception) {
                    Toast.makeText(this@LoginActivity, "فشل تسجيل الدخول", Toast.LENGTH_LONG).show()
                } finally {
                    loginBtn.isEnabled = true
                }
            }
        }
    }
}
