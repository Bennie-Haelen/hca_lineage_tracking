# hca_lineage_tracking
hca_lineage_tracking

Invokation examples:

1. Show lineage
python main.py --action show_lineage --project_id hca-sandbox  --location us --process_id dcaae7534de82f9e542f04f1b3e541c6 
            --run_id b0a38abef92d6de295d608a67194d363     


2. Search Target Lineage
python main.py --project_id hca-sandbox --location_id us --action show_target_lineage --dataset_id lineage_samples --table_id facility_inspection_report

3. Search Source Lineage
python main.py --project_id hca-sandbox --location_id us --action show_source_lineage --dataset_id lineage_samples --table_id facilities

4. Delete Lineage
python main.py --project_id hca-sandbox --location_id us --action delete_lineage --process_id 7c494c3e955705b453d5c3f41673b50c --run_id f91f816fc022862f8b9d6ec70aca9548 --lineage_event_id 7b218f89e29cbe4a588944815a9bb520

5. Delete all target lineage
python main.py --project_id hca-sandbox --location_id us --dataset_id lineage_samples --table_id facility_inspection_report
 --action delete_all_target_lineage

6. Retrieve Processes for Link
python main.py --action retrieve_processes_for_link  --project_id hca-sandbox --location_id




Run Dataflow:

python dataflow_lineage/dataflow_gcs_2.py --worker_zone=us-central1-a --no_use_public_ips 