import requests
import json
import time
from bs4 import BeautifulSoup

def get_job_details(job_id):
    url = f"https://technopark.in/job-details/{job_id}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(f" Failed to fetch job ID {job_id}")
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


def scrape_all_jobs():
    all_jobs = []
    page = 1

    while True:
        print(f"\nFetching page {page}...")
        api_url = f"https://technopark.in/api/paginated-jobs?page={page}"
        res = requests.get(api_url)
        
        if res.status_code != 200:
            print("Failed to fetch job list.")
            break

        data = res.json()
        jobs = data.get("data", [])

        if not jobs:
            break

        for job in jobs:
            job_id = job['id']
            title = job['job_title']
            company = job['company']['company']
            link = f"https://technopark.in/job-details/{job_id}"

            print(f" Scraping: {title} at {company}")
            brief_desc, skills, email = get_job_details(job_id)

            all_jobs.append({
                "job_title": title,
                "company": company,
                "brief_description": brief_desc,
                "preferred_skills": skills,
                "email": email,
                "job_link": link
            })

            time.sleep(1)  # To avoid hammering the server

        if not data.get("next_page_url"):
            break
        page += 1

    with open("job.json", "w", encoding="utf-8") as f:
        json.dump(all_jobs, f, ensure_ascii=False, indent=4)

    print(f"\n✅ Saved {len(all_jobs)} jobs to job.json")


if __name__ == "__main__":
    scrape_all_jobs()
