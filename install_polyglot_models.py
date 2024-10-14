import os

def download_polyglot_models():
    os.system("polyglot download sentiment2.en")
    os.system("polyglot download sentiment2.es")
    os.system("polyglot download sentiment2.pt")
    os.system("polyglot download sentiment2.uz")
    os.system("polyglot download sentiment2.fy")
    os.system("polyglot download sentiment2.ru")
    os.system("polyglot download sentiment2.zh")
    os.system("polyglot download sentiment2.aa")

if __name__ == "__main__":
    download_polyglot_models()