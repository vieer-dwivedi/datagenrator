import ray
import ray.data as rd
from datasets import load_dataset

def main():
    # Connect to Ray cluster inside Kubernetes
    ray.init(address="auto")

    print("Connected to Ray:", ray.cluster_resources())

    # Load dataset from Hugging Face
    hf_ds = load_dataset(
        "FrancoMango/yellow-taxi",
        split="train"
    )

    # Convert to Ray Dataset
    ds = rd.from_huggingface(hf_ds)

    # Simple validation
    ds = ds.filter(lambda r: r["trip_distance"] >= 0)

    # Write output (ephemeral for now)
    ds.write_parquet("/tmp/taxi_parquet")

    print("✅ Ingestion completed successfully")

if __name__ == "__main__":
    main()
