import os

INPUT_FILE = "F:\yelp-data\yelp_academic_dataset_review.json"      # your 5GB file
OUTPUT_DIR = "split_files"
NUM_PARTS = 20

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Step 1: Count total lines
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    total_lines = sum(1 for _ in f)

lines_per_file = total_lines // NUM_PARTS
extra = total_lines % NUM_PARTS

print(f"Total lines: {total_lines}")
print(f"Lines per file: {lines_per_file}")

# Step 2: Split the file
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    file_index = 1
    line_count = 0
    current_limit = lines_per_file + (1 if extra > 0 else 0)

    out = open(
        os.path.join(OUTPUT_DIR, f"reviews_part_{file_index}.json"),
        "w",
        encoding="utf-8"
    )

    for line in f:
        out.write(line)
        line_count += 1

        if line_count >= current_limit:
            out.close()
            file_index += 1
            line_count = 0
            extra -= 1

            if file_index > NUM_PARTS:
                break

            current_limit = lines_per_file + (1 if extra > 0 else 0)
            out = open(
                os.path.join(OUTPUT_DIR, f"reviews_part_{file_index}.json"),
                "w",
                encoding="utf-8"
            )

    out.close()

print("Splitting completed!")
