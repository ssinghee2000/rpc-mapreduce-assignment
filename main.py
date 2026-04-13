import requests
import time
import string
from multiprocessing import Pool, cpu_count
from collections import Counter
import requests.exceptions


BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "MDS202534"

def login(student_id): # helps to get the secret key
    url = f"{BASE_URL}/login"
    payload = {"student_id": student_id}

    response = requests.post(url, json=payload,timeout=5)
    
    if response.status_code != 200:
        raise Exception(f"Login failed: {response.status_code}, {response.text}")
    
    return response.json()["secret_key"]

# secret_key = login(STUDENT_ID) # fetches secret_key
import re

def extract_first_word(title):
    if not title:
        return None

    word = title.split()[0]  # strictly first token
    word = word.lower().strip(string.punctuation)

    return word if word else None


def get_title_with_retry(secret_key, filename, max_retries=5):
    url = f"{BASE_URL}/lookup"
    payload = {
        "secret_key": secret_key,
        "filename": filename
    }

    retries = 0

    while retries < max_retries:
        response = requests.post(url, json=payload,timeout=5)

        if response.status_code == 200:
            return response.json()["title"]

        elif response.status_code == 429:
            # Too many requests → wait and retry
            time.sleep(0.2)
            retries += 1

        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    raise Exception("Max retries exceeded")

def mapper(filename_chunk):
    """
    Map phase:
    - Each worker logs in once
    - Processes a chunk of filenames
    - Returns a Counter of first-word frequencies
    """
    counter = Counter()
    secret_key = login(STUDENT_ID)
    for filename in filename_chunk:
        url = f"{BASE_URL}/lookup"
        payload = {
            "secret_key": secret_key,
            "filename": filename
        }
        while True:
            try:
                response = requests.post(url, json=payload, timeout=5)
                if response.status_code == 200:
                    title = response.json()["title"]
                    break
                elif response.status_code == 429:
                    time.sleep(0.3)  # slightly more delay
                else:
                    print(f"Error {response.status_code}: {response.text}")
                    continue
            except requests.exceptions.RequestException:
                print("Connection issue, retrying...")
                time.sleep(0.5)
        if title:
            word = extract_first_word(title)
            if word:
                counter[word] += 1
    return counter

def verify_top_10(STUDENT_ID, top_10_list):
    """
    Verify final result with server
    """
    secret_key = login(STUDENT_ID)
    url = f"{BASE_URL}/verify"
    payload = {
        "secret_key": secret_key,
        "top_10": top_10_list
    }
    response = requests.post(url, json=payload,timeout=5)
    if response.status_code == 200:
        print("\n Verification Result:")
        print(response.json())
    else:
        print(f" Verification failed: {response.status_code}, {response.text}")

def chunkify(data, n_chunks):
    chunk_size = len(data) // n_chunks
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

if __name__ == "__main__":
    # Step 1: Generate filenames
    filenames = [f"pub_{i}.txt" for i in range(1000)]
    # Step 2: Decide number of workers. Its important that we avoid to use too many
    num_workers = 2  # keep it safe for rate limits
    #Step3: Split into chunks
    chunks = chunkify(filenames, num_workers)

    # Step 4: Map phase (parallel execution)
    with Pool(num_workers) as pool:
        results = pool.map(mapper, chunks)
    # Step 5: Reduce phase (combine results)
    final_counter = Counter()
    for partial_counter in results:
        final_counter.update(partial_counter)
    print("Total words counted:", sum(final_counter.values()))    
    # Step 6: Get top 10
    top_10 = [word.capitalize() for word, _ in final_counter.most_common(10)]
    print("\nTop 10 Most Frequent First Words:")
    print(top_10)
    if top_10:
        verify_top_10(STUDENT_ID, top_10)
    else:
        print("Compute the top 10 words first!")

    # except Exception as e:
    #     print("Error:", e)
