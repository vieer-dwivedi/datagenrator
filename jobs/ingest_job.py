import ray
import ray.data as rd

def main():
    # 1️⃣ Connect to Ray cluster
    ray.init(address="auto")

    print("Connected to Ray:", ray.cluster_resources())

    # 2️⃣ READ DATA USING RAY (NOT HuggingFace load_dataset)
    # This enables parallel ingestion
    ds = rd.read_huggingface(
        "FrancoMango/yellow-taxi",
        split="train",
        streaming=True   # 🔥 CRITICAL: enables parallel streaming
    )

    # 3️⃣ PROVE parallelism
    print("Initial Ray blocks:", ds.num_blocks())

    # 4️⃣ Optional: force more parallelism
    ds = ds.repartition(8)
    print("Blocks after repartition:", ds.num_blocks())

    # 5️⃣ Parallel transformation
    ds = ds.filter(lambda r: r["trip_distance"] >= 0)

    # 6️⃣ Materialize in parallel
    ds.write_parquet("/tmp/taxi_parquet")

    print("✅ Ingestion completed successfully")

if __name__ == "__main__":
    main()
