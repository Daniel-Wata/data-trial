from textblob import TextBlob
import logging
def get_sentiment_textblob(text):
    if not text:
        return None
    
    blob = TextBlob(text)
    try:
        # Only translate if the language is not English
        if blob.detect_language() != 'en':
            blob = blob.translate(to='en')
    except Exception as e:
        # Log the exception for debugging
        #logging.warning(f"Translation failed for text: {text[:100]}... Exception: {str(e)}")
        pass  # If translation fails, use the original text
    return blob.sentiment.polarity



def get_sentiment_polyglot(text):
    from polyglot.text import Text
    try:
        polyglot_text = Text(text)
        scores = [w.polarity for w in polyglot_text.words if w.polarity != 0]
        
        # Check if there are any sentiment scores
        if len(scores) > 0:
            return sum(scores) / float(len(scores))
        else:
            return None
    except Exception as e:
        print(f"Error processing text: {text} - {str(e)}")
        return None 