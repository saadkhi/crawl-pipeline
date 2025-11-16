# GitHub Stars Tracker

A robust pipeline for tracking GitHub repository stars over time using GitHub's GraphQL API and PostgreSQL for data storage. This tool helps monitor star growth and analyze repository popularity trends.

## ✨ Features

- **Automated Data Collection**: Scheduled crawls to track star counts over time
- **Efficient Storage**: PostgreSQL database for structured data storage
- **Resilient**: Built-in retry mechanism with exponential backoff
- **Scalable**: Handles pagination for large result sets
- **Flexible Querying**: Track any GitHub repositories based on search criteria
- **Data Export**: Easily export star history to CSV for analysis

## 🛠️ Prerequisites

- Python 3.11+
- PostgreSQL database (compatible with Neon.tech)
- GitHub Personal Access Token with `public_repo` scope
- Required Python packages (see [requirements.txt](requirements.txt))

## 🚀 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/crawl-pipeline.git
   cd crawl-pipeline
   ```

2. Create a virtual environment and activate it:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   ```bash
   # Required
   export GITHUB_TOKEN=your_github_token
   export DATABASE_URL=postgresql://user:password@host:port/dbname
   ```

## 🏗️ Database Setup

<video src="media/neon-video.webm" controls width="100%">
  Your browser does not support the video tag.
</video>

This application requires a PostgreSQL database with the following schema (automatically created on first run):

- **repos**: Stores repository metadata
- **repo_stars**: Tracks star counts over time
- **repo_meta**: Flexible JSON storage for additional repository metadata
- **crawl_progress**: Maintains cursor position for pagination

To manually set up the database, run:
```bash
python -c "from crawl_stars import ensure_tables; ensure_tables()"
```

## 🏃‍♂️ Usage

### Manual Execution

Run the crawler manually:
```bash
python crawl_stars.py
```

### Export Data

Export star history to CSV:
```bash
python dump_data.py
```
This creates a `repo_stars.csv` file with the latest star counts.

### GitHub Actions

The workflow is configured to run daily at 2 AM UTC. You can also trigger it manually from the Actions tab.

![GitHub Actions Workflow](github-action-workflow-complete.png)
*Screenshot showing successful workflow execution in GitHub Actions*

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GITHUB_TOKEN` | GitHub Personal Access Token | ✅ Yes |
| `DATABASE_URL` | PostgreSQL connection string | ✅ Yes |

### Customizing the Search Query

Modify the search query in `crawl_stars.py` (line 180) to track different repositories:

```python
# Example: Track repositories with more than 1000 stars
crawl_once("stars:>1000", start_cursor=last_cursor, max_pages=5)
```

## 📊 Database Schema

### repos
- `id`: GitHub's node ID (primary key)
- `owner`: Repository owner
- `name`: Repository name
- `full_name`: owner/name format
- `url`: Repository URL
- `description`: Repository description
- `language`: Primary programming language
- `default_branch`: Default branch name
- `first_seen_at`: First crawl timestamp
- `last_seen_at`: Last crawl timestamp
- `updated_at`: Last update from GitHub

### repo_stars
- `id`: Auto-incrementing ID
- `repo_id`: Foreign key to repos.id
- `observed_at`: Timestamp of observation
- `stargazers`: Number of stars at observation time

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- GitHub for the GraphQL API
- PostgreSQL for the robust database system
- The Python community for the excellent libraries used in this project
