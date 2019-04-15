import hashlib
import json
import textblob

def force_unicode(m):
    try:
        return unicode(m)
    except UnicodeDecodeError:
        ascii = str(m).encode('string_escape')
        return unicode(ascii)

def parse_review_body(txt):
    """
    Parse reviews provided by textual body (one review per line).
    """
    lines = txt.split("\n")
    records = []
    for line in lines:
        # forcefully handle encoding issues
        line = force_unicode(line.strip())
        if line == "": continue
        records.append(parse_review(line))
    jsonout = json.dumps(records)
    return jsonout

def parse_review(line):
    """
    Parses a review of format: <PRODUCT-ID> Review: <REVIEW-TEXT>.
    Extracts the following attributes and textual features:
    
    Extracts product ID (ID), text length (LENGTH), text (TEXT), sentiment polarity (POLARITY), and
    sentiment subjectivity (SUBJECTIVITY). Returns information as a dictinary.
    """
    try:
        md5 = hashlib.md5(line).hexdigest()
        rid, text = line.split(": ", 1)
        tb = textblob.TextBlob(text)
        return { "ID": rid[:7],
                 "MD5": md5,
                 "LENGTH": len(text), 
                 "TEXT": force_unicode(line),
                 "POLARITY": tb.polarity,
                 "SUBJECTIVITY": tb.subjectivity }
    except ValueError, e:
        raise ValueError("Line does not match expceted format \"<PRODUCT-ID> Review: <REVIEW-TEXT>\"; LINE: \"%s\"; ERROR: %s" % (line, str(e)))
    except Exception, e:
        # just forward
        raise e

# ////////////////////////////////////////////////////////////
# Wrap parser in python operator
# ////////////////////////////////////////////////////////////

def on_input(msg):
    # inform downstream operators about last file:
    # set message.commit.token = 1 for last file
    commit_token = "0"
    if msg.attributes["storage.endOfSequence"]:
        commit_token = "1"
    
    # parse the line-based input    
    parsed_as_json = parse_review_body(msg.body)
    
    output_message = api.Message(parsed_as_json, {"message.commit.token": commit_token})
    api.send("output", output_message)

api.set_port_callback("input", on_input)