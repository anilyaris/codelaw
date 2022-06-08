import re
import ahocorasick

keywords = ["gdpr", "rgpd", "dsgvo", "ccpa", "cpra", "privacy", "data_protection", "compliance", "legal", "consent", "law", "statute", "personal_data", "comply", "hipaa", "fcra", "ferpa", "glba", "ecpa", "coppa", "vppa"]
A = ahocorasick.Automaton()
for keyword in keywords:
    A.add_word(keyword.replace("data", "").replace("_", ""), keyword)
A.make_automaton()

def match_keywords(text, labels, quantifier = ""):
    if text:
        text = text.lower()
        for end_index, original_value in A.iter(text):
            match = "_" not in original_value
            if not match:
                if original_value[0] == "p": # personal_data
                    match = "data" in text[end_index + 1:end_index + 6]
                elif original_value[0] == "d": # data_protection
                    match = "data" in text[end_index -14:end_index - 9]
            if match:
                labels[original_value + quantifier] = True
                    

def generate_labels(patch = "", labels = {}, quantifiers = [""]):
    if not labels:
        for k in keywords:
            for q in quantifiers:
                labels[k + q] = False
    
    for line in patch.split('\n'):
        if line:
            added = line[0] == '+'
            removed = line[0] == '-'
            if added or removed:

                for c in line:
                    if c.isalpha():     # only try to match if line has alphanumeric characters
                        match_keywords(line, labels, "_added" if added else "_removed")                            
                        break   # from alphanumeric check

    return labels

if __name__ == '__main__':
    s = "+#GDPR for legal issues\n-rgpdVar = 5\n+someVar = true  # for personaldata protection\n- \n-  ()  \n-legality = false"
    labels = generate_labels(s)
    from pprint import pprint
    pprint(labels)