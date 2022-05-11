import re

keywords = ["gdpr", "rgpd", "dsgvo", "ccpa", "cpra", "privacy", "data_protection", "compliance", "legal", "consent", "law", "statute", "personal_data"]

def match_keyword(text, keyword):
    pattern = ''
    for subword in keyword.split('_'):
        pattern += subword
        pattern += '.?'
    pattern = pattern[:-2]
    match = re.search(pattern, text, re.IGNORECASE)
    return bool(match)

def generate_labels(patch, labels = {}):
    if not labels:
        for k in keywords:
            labels[k + "_added"] = False
            labels[k + "_removed"] = False

    for line in patch.split('\n'):
        if line:
            added = line[0] == '+'
            removed = line[0] == '-'
            if added or removed:

                for c in line:
                    if c.isalpha():     # only try to match if line has alphanumeric characters
                        for keyword in keywords:
                            key = keyword + ("_added" if added else "_removed")
                            labels[key] = labels[key] or match_keyword(line, keyword)
                            
                        break   # from alphanumeric check

    return labels

if __name__ == '__main__':
    s = "+#GDPR for legal issues\n-rgpdVar = 5\n+someVar = true  # for data protection\n- \n-  ()  \n-legality = false"
    labels = generate_labels(s)
    from pprint import pprint
    pprint(labels)