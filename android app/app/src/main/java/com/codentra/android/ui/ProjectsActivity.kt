package com.codentra.android.ui

import android.os.Bundle
import android.widget.Button
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.codentra.android.R
import com.codentra.android.api.ApiClient
import com.codentra.android.model.Project
import com.codentra.android.util.SessionStore
import kotlinx.coroutines.launch

class ProjectsActivity : AppCompatActivity() {
    private lateinit var session: SessionStore
    private lateinit var adapter: ProjectsAdapter

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_projects)

        session = SessionStore(this)
        val recycler = findViewById<RecyclerView>(R.id.projectsList)
        val title = findViewById<TextView>(R.id.screenTitle)
        val logout = findViewById<Button>(R.id.logoutBtn)

        title.text = "Codentra - Projects"
        adapter = ProjectsAdapter()
        recycler.layoutManager = LinearLayoutManager(this)
        recycler.adapter = adapter

        logout.setOnClickListener {
            session.clear()
            finish()
        }

        loadProjects()
    }

    private fun loadProjects() {
        lifecycleScope.launch {
            try {
                val api = ApiClient.create(session)
                val resp = api.projects()
                adapter.submit(resp.projects)
            } catch (ex: Exception) {
                Toast.makeText(this@ProjectsActivity, "فشل تحميل المشاريع من السيرفر", Toast.LENGTH_LONG).show()
            }
        }
    }
}

class ProjectsAdapter : RecyclerView.Adapter<ProjectVH>() {
    private val items = mutableListOf<Project>()

    fun submit(newItems: List<Project>) {
        items.clear()
        items.addAll(newItems)
        notifyDataSetChanged()
    }

    override fun onCreateViewHolder(parent: android.view.ViewGroup, viewType: Int): ProjectVH {
        val view = android.view.LayoutInflater.from(parent.context).inflate(R.layout.item_project, parent, false)
        return ProjectVH(view)
    }

    override fun getItemCount(): Int = items.size

    override fun onBindViewHolder(holder: ProjectVH, position: Int) = holder.bind(items[position])
}

class ProjectVH(itemView: android.view.View) : RecyclerView.ViewHolder(itemView) {
    fun bind(project: Project) {
        itemView.findViewById<TextView>(R.id.projectTitle).text = project.title ?: "Untitled"
        itemView.findViewById<TextView>(R.id.projectDesc).text = project.description ?: ""
        itemView.findViewById<TextView>(R.id.projectPrice).text = "$ ${project.price ?: 0.0}"
    }
}
