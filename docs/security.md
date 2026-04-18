# Security Basics

This project will use environment variables to store configuration such as database credentials, service names, ports, and future API keys.

The `.env.example` file provides a safe template for required variables, while the real `.env` file stays local and must not be committed to GitHub. No real personal or sensitive customer data will be used in this project.

The project will also avoid hardcoding secrets in Python scripts, Docker files, or notebooks. This helps build good security habits from the start and keeps the repository safe to share publicly.
