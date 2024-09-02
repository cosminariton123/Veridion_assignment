import io
from tqdm import tqdm

def parse_facebook(filepath):

    with io.open(filepath, "r", encoding="utf-8") as f:
        file = f.read()
        
    file = file.splitlines()

    headers = file[0].split(",")
    file = file[1:]

    result_lines = []
    for line in tqdm(file):
        between_qoutes = False
        data = []
        word = ""

        for chr, chr_2 in zip(line, line[1:]):
            if chr == "\\":
                continue

            if chr == "\"":
                between_qoutes = not between_qoutes
                continue
            
            if between_qoutes is True:
                word += chr
                continue

            if chr == "," and chr_2 != " " and not between_qoutes:
                data.append(word)
                word = ""
                continue
            
            word += chr

        if line[-1] != "\"": word += line[-1]
        data.append(word)

        result_lines.append(data)
        
        try:
            assert len(result_lines[-1]) == len(headers), f"Line should have length {len(headers)} but has {len(result_lines[-1])}.\n Headers: {headers}.\n Line is {result_lines[-1]}"
        except:
            print(f"Line should have length {len(headers)} but has {len(result_lines[-1])}.\n Headers: {headers}.\n Line is {result_lines[-1]}")

    print("Done")