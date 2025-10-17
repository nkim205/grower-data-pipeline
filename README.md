# grower-data-pipeline

Data pipeline that takes power outage data from S3 and transforms it into SAIDI/SAIFI.

---

# 🌿 Creating a Feature Branch from `origin/dev`

This guide walks you through the standard process for creating a new feature branch from the `origin/dev` branch. Follow these steps to ensure your branch correctly tracks the remote development branch and follows best practices.

> **TL;DR**
>
> 1. Be on an up-to-date `dev`, 2) create your feature branch off `dev`, 3) push with `-u`, 4) open a PR back into `dev`.

---

## 🚧 Prerequisites

* Git installed and configured
* Access to the repository (HTTPS or SSH)
* You know the repo URL (ask a teammate if unsure)

---

## 🚀 Step-by-Step Guide

### 1) Clone the repository (if you haven’t already)

```bash
git clone <repository-url>
cd <repository-name>
```

### 2) Fetch the latest changes from remote

Ensure your local copy is aware of the latest branches and commits.

```bash
git fetch origin
```

### 3) Check out the `dev` branch

If your local `dev` branch doesn’t exist yet, create it from the remote `origin/dev` branch.

```bash
git checkout -b dev origin/dev
```

### 4) Verify the tracking relationship

```bash
git branch -vv
```

You should see output similar to:

```
* dev    a1b2c3d [origin/dev] Your latest commit message
```

If it’s not tracking, set it manually with:

```bash
git branch --set-upstream-to=origin/dev dev
```

### 5) Make sure your local `dev` branch is fully up to date

This ensures your feature branch starts from the most recent development code (**VERY IMPORTANT**).

```bash
git pull origin dev
```

### 6) Create a new branch from `dev`

Use a descriptive name that reflects the work you’ll be doing.

```bash
git checkout -b <your_name>/<feature-name>
```

This creates a local feature branch based on your current (updated) `dev` branch.

### 7) Push the new feature branch to remote

Push your new branch to the remote repository and set it to track automatically:

```bash
git push -u origin <your_name>/<feature-name>
```

The `-u` flag sets the upstream branch so future `git push` and `git pull` commands automatically know which remote branch to use.

### 8) Start developing 🚧

You can now begin implementing your feature. Make regular commits and push your changes frequently to keep your branch updated.

```bash
git add .
git commit -m "Implement <concise change description>"
git push
```

### 9) Open a Pull Request (PR)

Once your feature is ready to merge:

1. Go to your repository on GitHub.
2. Open a Pull Request from `<your_name>/<feature-name>` → `dev`.
3. Add a descriptive title and summary of the changes.
4. Request reviews from your teammates if applicable.
5. Your PR will be reviewed and, once approved, merged into `dev`.

---