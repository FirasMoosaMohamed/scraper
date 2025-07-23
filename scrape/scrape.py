import requests
import json
import time
from bs4 import BeautifulSoup
import redis
import hashlib
import random
from publish import publish_jobs_to_rabbitmq

# --- Redis Configuration ---
# Connect to your Redis server. Adjust host, port, db, and password as needed.
r = redis.Redis(host='localhost', port=6379, db=1)

# --- Function to generate a unique hash for a job ---
def generate_job_hash(job_data):
    """Generates a unique SHA256 hash for a job based on its core fields."""
    # Using job_title, company, and job_link as primary unique identifiers.
    # It's crucial that these fields are consistently formatted (e.g., lowercase, stripped whitespace)
    # to avoid different hashes for essentially the same job.
    
    # Ensure all parts are strings before concatenation
    title = str(job_data.get('job_title', '')).lower().strip()
    company = str(job_data.get('company', '')).lower().strip()
    link = str(job_data.get('job_link', '')).strip()

    unique_string = f"{title}-{company}-{link}"
    
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

# --- Original get_job_details function (no changes needed here) ---
def get_job_details(job_id):
    url = f"https://technopark.in/job-details/{job_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
    }

    try:
        res = requests.get(url, headers=headers, timeout=10) # Added timeout
        res.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f" Failed to fetch job ID {job_id}: {e}")
        return "N/A", "N/A", "N/A"

    soup = BeautifulSoup(res.text, 'html.parser')

    # Brief Description
    brief_description = "N/A"
    for h3 in soup.find_all("h3"):
        if "brief description" in h3.text.strip().lower():
            desc_div = h3.find_next_sibling("div")
            if desc_div:
                brief_description = desc_div.get_text(strip=True)
            break

    # Preferred Skills
    preferred_skills = "N/A"
    for h3 in soup.find_all("h3"):
        if "preferred skills" in h3.text.strip().lower():
            skills_div = h3.find_next_sibling("div")
            if skills_div:
                preferred_skills = skills_div.get_text(strip=True)
            break

    # Email
    email = "N/A"
    email_tag = soup.find("a", href=lambda h: h and h.startswith("mailto:"))
    if email_tag:
        email = email_tag['href'].replace("mailto:", "").strip()

    return brief_description, preferred_skills, email

# --- Modified scrape_all_jobs function ---
def scrape_all_jobs():
    # We will no longer append to `all_jobs` list in memory for Redis storage.
    # Instead, we directly store to Redis and potentially retrieve them later for a local JSON export.
    
    page = 1
    new_jobs_count = 0
    total_jobs_processed = 0

    print("--- Starting Job Scrape and Redis Integration ---")

    while True:
        print(f"\nFetching page {page}...")
        api_url = f"https://technopark.in/api/paginated-jobs?page={page}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
        }

        try:
            res = requests.get(api_url, headers=headers, timeout=10) # Added timeout
            res.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        except requests.exceptions.RequestException as e:
            print(f" Failed to fetch job list for page {page}: {e}")
            break

        data = res.json()
        jobs = data.get("data", [])

        if not jobs:
            print("No more jobs found on this page. Ending scrape.")
            break

        for job_summary in jobs:
            total_jobs_processed += 1
            job_id = job_summary['id']
            title = job_summary['job_title']
            company = job_summary['company']['company']
            link = f"https://technopark.in/job-details/{job_id}"

            # Create a dictionary for the current job
            current_job = {
                "job_title": title,
                "company": company,
                "job_link": link # Use this for hashing
            }

            # Generate a unique hash for this job
            job_hash = generate_job_hash(current_job)

            # Check if this job (hash) is already in our Redis set
            # 'SADD' returns 1 if added (new), 0 if already exists.
            if r.sadd("processed_job_hashes", job_hash):
                # Job is NEW, proceed to scrape details and store
                print(f" NEW Job: Scraping details for '{title}' at '{company}'...")
                brief_desc, skills, email = get_job_details(job_id)

                full_job_data = {
                    "job_title": title,
                    "company": company,
                    "brief_description": brief_desc,
                    "preferred_skills": skills,
                    "email": email,
                    "job_link": link,
                    "scraped_at": int(time.time()) # Add timestamp for when it was scraped
                }

                # Store the full job data as a Redis Hash
                # Convert list to JSON string for storage in Redis Hash
                job_data_for_redis = {
                    k: json.dumps(v) if isinstance(v, list) else v for k, v in full_job_data.items()
                }
                # Store using a key like "job:<job_hash>"
                r.hmset(f"job:{job_hash}", job_data_for_redis)
                publish_jobs_to_rabbitmq(job_data_for_redis)
                # Add this job's hash to a list for easy retrieval of all job hashes
                r.rpush("all_job_hashes", job_hash)
                
                new_jobs_count += 1
            else:
                print(f" DUPLICATE Job: '{title}' at '{company}' (Skipped)")

            time.sleep(random.randint(3,5))  # Be polite to the server, adjusted sleep slightly

        if not data.get("next_page_url"):
            print("No more pages indicated. Ending scrape.")
            break
        page += 1

    print(f"\n--- Scrape Complete ---")
    print(f"Total jobs processed (including duplicates): {total_jobs_processed}")
    print(f"Total NEW jobs added to Redis: {new_jobs_count}")

    # --- Optional: Export all jobs from Redis to a local JSON file ---
    export_jobs_from_redis_to_json("jobs_from_redis.json")

def export_jobs_from_redis_to_json(filename):
    """Retrieves all jobs from Redis and saves them to a JSON file."""
    all_job_hashes_in_redis = r.lrange("all_job_hashes", 0, -1)
    exported_jobs = []

    print(f"\nExporting {len(all_job_hashes_in_redis)} jobs from Redis to {filename}...")

    for h_bytes in all_job_hashes_in_redis:
        job_hash = h_bytes.decode('utf-8')
        job_data_bytes = r.hgetall(f"job:{job_hash}")
        
        # Decode byte strings and convert JSON strings back to lists/objects
        job_data = {}
        for k_bytes, v_bytes in job_data_bytes.items():
            key = k_bytes.decode('utf-8')
            value = v_bytes.decode('utf-8')
            try:
                # Try to load as JSON for fields that might be lists/objects (like preferred_skills)
                job_data[key] = json.loads(value)
            except json.JSONDecodeError:
                job_data[key] = value # If not JSON, keep as string
        exported_jobs.append(job_data)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(exported_jobs, f, ensure_ascii=False, indent=4)

    print(f"✅ Exported {len(exported_jobs)} jobs to {filename}")


if __name__ == "__main__":
    # Ping Redis to ensure connection
    try:
        r.ping()
        print("Successfully connected to Redis! 🎉")
    except redis.exceptions.ConnectionError as e:
        print(f"🚨 Could not connect to Redis: {e}")
        print("Please ensure Redis server is running and accessible.")
        exit() # Exit if Redis is not connected

    scrape_all_jobs()