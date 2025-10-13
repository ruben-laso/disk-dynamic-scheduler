import re
from pathlib import Path

def propagate_swaps_to_expanded(base_dot):
    """
    For a given base .dot file (e.g., atacseq.dot),
    find all expanded files (e.g., atacseq_1000.dot, atacseq_2000.dot, ...)
    and copy 'swaps' attributes from base tasks to their numbered counterparts.
    """
    base_path = Path(base_dot)
    base_stem = base_path.stem  # e.g. 'atacseq'
    base_dir = base_path.parent

    # 1️⃣ Read base file and extract swaps mapping
    base_text = base_path.read_text()
    base_pattern = re.compile(r'\[label\s*=\s*(?:"|)([A-Za-z0-9_]+)(?:"|)[^]]*swaps\s*=\s*([a-z]+)', re.I)
    swaps_map = {task: swaps for task, swaps in base_pattern.findall(base_text)}

    if not swaps_map:
        print(f"⚠️ No 'swaps' found in {base_dot}")
        return

    print(f"✅ Found {len(swaps_map)} swap annotations in {base_dot}")

    # 2️⃣ Find all matching expanded files in same directory
    expanded_files = sorted(base_dir.glob(f"{base_stem}_*.dot"))
    if not expanded_files:
        print(f"⚠️ No expanded files found for {base_stem}")
        return

    print(f"📁 Found {len(expanded_files)} expanded files to process.")

    # 3️⃣ Process each expanded file
    for expanded_path in expanded_files:
        lines = expanded_path.read_text().splitlines()
        updated_lines = []

        for line in lines:
            if '->' in line or '[' not in line:
                updated_lines.append(line)
                continue

            # Extract label like BWA_MEM_00000004
            match = re.search(r'label\s*=\s*"?(?P<label>[A-Za-z0-9_]+)"?', line)
            if not match:
                updated_lines.append(line)
                continue

            full_label = match.group('label')
            base_name = re.sub(r'_\d+$', '', full_label)

            if base_name in swaps_map and 'swaps=' not in line:
                swaps_value = swaps_map[base_name]
                line = re.sub(r'(\])', f', swaps={swaps_value}\\1', line)

            updated_lines.append(line)

        expanded_path.write_text('\n'.join(updated_lines))
        print(f"  ✏️ Updated {expanded_path.name}")

    print("🎯 All expanded files processed successfully.")

# Example usage:
propagate_swaps_to_expanded("methylseq.dot")
