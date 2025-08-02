
import requests, os

def generate_response(
    prompt: str,
    mode: str = "completion",  # "chat" or "completion"
    server_url: str = f"http://{os.getenv('INFERENCE_URL')}",
    max_tokens: int = 512,
    temperature: float = 0
):
    if mode == "chat":
        url = f"{server_url}/v1/chat/completions"
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
    elif mode == "completion":
        url = f"{server_url}/v1/completions"
        payload = {
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": temperature
        }
    else:
        raise ValueError("mode must be 'chat' or 'completion'")

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        result = response.json()

        return result

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def extract_urls(retriever_results):
    return [
        doc.get("metadata", {}).get("url")
        for doc in retriever_results
        if doc.get("metadata", {}).get("url")  # filter out None or missing URLs
    ]