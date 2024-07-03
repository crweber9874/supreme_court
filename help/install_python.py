which python
#brew install python3
#brew install virtualenv
cd py
virtualenv .venv
source .venv/bin/activate

pip install requests lxml beautifulsoup4 pandas numpy regex nltk
google-cloud-bigquery google-cloud-storage zipfile36 Python-IO
oauth2client google-colab
gcloud auth application-default set-quota-project $(gcloud config get-value project)
gcloud config set project supremeCourt

# create a requirement.txt
pip freeze > requirements.txt

gcloud auth application-default login --impersonate-service-account your-service-christopher.weber9874@supremeCourt.iam.gserviceaccount.com

