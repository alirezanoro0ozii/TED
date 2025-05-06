# pip install --upgrade google-auth
# curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-471.0.0-linux-x86_64.tar.gz
# tar -xf google-cloud-cli-471.0.0-linux-x86_64.tar.gz
# ./google-cloud-sdk/install.sh
# ./google-cloud-sdk/bin/gcloud init
# python dataset_export.py --dataset 4_taxseq_new
# ./google-cloud-sdk/bin/gsutil -m cp -r gs://export_pqt_4_taxseq_new .
python dataset_indexer.py --dataset_folder /home/aac/TED/dataset/export_pqt_0_ted/
