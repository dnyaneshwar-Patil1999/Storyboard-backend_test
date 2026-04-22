# ======================================================================================
# === INDEXER REBUILD SCRIPT WITH INCREMENTAL UPDATE OPTION ============================
# ======================================================================================

import os
import json
import time
import requests
from datetime import datetime, timezone
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchField, SearchIndex, SearchFieldDataType, ComplexField,
    VectorSearch, VectorSearchProfile,
    SemanticConfiguration, SemanticField, SemanticPrioritizedFields,
    SearchIndexerDataSourceConnection,
    SearchIndexer, FieldMapping, IndexingParameters
)
from azure.core.exceptions import HttpResponseError
from typing import Optional

# --- CONFIGURATION ---
class IndexerConfig:
    """Configuration settings for the Azure AI Search indexer."""
    def __init__(self):
        # General Settings
        self.FULL_REBUILD = os.getenv("FULL_REBUILD", "False").lower() == "true" # Convert to boolean
        self.API_VERSION = os.getenv("API_VERSION", "2023-11-01")

        # Azure AI Search Service
        self.SEARCH_ENDPOINT = os.getenv("SEARCH_ENDPOINT")
        self.SEARCH_API_KEY = os.getenv("SEARCH_API_KEY")

        # Azure Blob Storage
        self.STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
        self.STORAGE_CONTAINER_NAME = os.getenv("STORAGE_CONTAINER_NAME", "aptara-processed-chunks")

        # Azure OpenAI Configuration
        self.AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
        self.AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME", "text-embedding-ada-002")
        self.VECTOR_DIMENSION = int(os.getenv("VECTOR_DIMENSION", "1536")) # Ensure integer

        # Resource Names (Make these configurable if needed)
        self.INDEX_NAME = os.getenv("INDEX_NAME", "pdf-self-contained-vector-index")
        self.DATASOURCE_NAME = os.getenv("DATASOURCE_NAME", "pdf-self-contained-datasource")
        self.SKILLSET_NAME = os.getenv("SKILLSET_NAME", "pdf-self-contained-skillset")
        self.INDEXER_NAME = os.getenv("INDEXER_NAME", "pdf-self-contained-indexer")
        self.VECTOR_SEARCH_PROFILE_NAME = os.getenv("VECTOR_SEARCH_PROFILE_NAME", "my-vector-profile")
        self.VECTOR_ALGORITHM_CONFIG_NAME = os.getenv("VECTOR_ALGORITHM_CONFIG_NAME", "my-hnsw-algorithm")
        self.SEMANTIC_CONFIG_NAME = os.getenv("SEMANTIC_CONFIG_NAME", "self-contained-semantic-config")

        # Indexer Monitoring Parameters
        self.INDEXER_RUN_TIMEOUT_MINUTES = int(os.getenv("INDEXER_RUN_TIMEOUT_MINUTES", "5")) # Max wait time for indexer to complete
        self.INDEXER_POLLING_INTERVAL_SECONDS = int(os.getenv("INDEXER_POLLING_INTERVAL_SECONDS", "10")) # How often to check status
        # New parameter: How long to tolerate a 'running' status with 0 items before stopping
        self.INDEXER_ZERO_ITEM_RUN_THRESHOLD_SECONDS = int(os.getenv("INDEXER_ZERO_ITEM_RUN_THRESHOLD_SECONDS", "5")) # Default 3 minutes


# Initialize Azure AI Search clients (outside the main functions for reuse)
def initialize_clients(config: IndexerConfig) -> tuple[SearchIndexClient, SearchIndexerClient]:
    """Initializes the Azure Search clients using the config."""
    try:
        index_client = SearchIndexClient(endpoint=config.SEARCH_ENDPOINT, credential=AzureKeyCredential(config.SEARCH_API_KEY))
        indexer_client = SearchIndexerClient(endpoint=config.SEARCH_ENDPOINT, credential=AzureKeyCredential(config.SEARCH_API_KEY))
        print("✅ Azure Search clients initialized successfully.")
        return index_client, indexer_client
    except Exception as e:
        print(f"❌ Failed to initialize Azure Search clients: {e}")
        raise  # Re-raise to signal critical failure

# ==============================================================================
# --- HELPER FUNCTIONS (Moved inside a class for better organization) ---
# ==============================================================================
class IndexerOperations:
    """
    Encapsulates the Azure AI Search indexer operations, making it modular
    and testable.
    """
    def __init__(self, index_client: SearchIndexClient, indexer_client: SearchIndexerClient, config: IndexerConfig):
        self.index_client = index_client
        self.indexer_client = indexer_client
        self.config = config

    def delete_component(self, component_type: str, component_name: str) -> None:
        """Deletes a component (indexer, skillset, data source, or index)."""
        try:
            if component_type == "indexer":
                self.indexer_client.delete_indexer(component_name)
                print(f"  - Indexer '{component_name}' deleted.")
            elif component_type == "skillset":
                requests.delete(f"{self.config.SEARCH_ENDPOINT}/skillsets/{component_name}?api-version={self.config.API_VERSION}", headers={"api-key": self.config.SEARCH_API_KEY})
                print(f"  - Skillset '{component_name}' deleted.")
            elif component_type == "datasource":
                self.indexer_client.delete_data_source_connection(component_name)
                print(f"  - Data source '{component_name}' deleted.")
            elif component_type == "index":
                self.index_client.delete_index(component_name)
                print(f"  - Index '{component_name}' deleted.")
            else:
                print(f"  - Unknown component type: {component_type}. Skipping deletion.")
                return
        except (HttpResponseError, requests.exceptions.RequestException) as e:
            # Handle 404 (Not Found) gracefully, print other errors
            if (isinstance(e, HttpResponseError) and e.status_code == 404) or \
               (isinstance(e, requests.exceptions.RequestException) and "404" in str(e)):
                print(f"  - Info: {component_type.capitalize()} '{component_name}' not found, skipping deletion.")
            else:
                print(f"  - Error deleting {component_type.capitalize()} '{component_name}': {e}")
                raise # Re-raise for other errors


    def create_or_update_index(self) -> None:
        """Creates or updates the search index."""
        print(f"\n--- Step 1: Creating/updating search index '{self.config.INDEX_NAME}' ---")
        fields = [
            SearchField(name="id", type=SearchFieldDataType.String, key=True, is_retrievable=True, filterable=True, sortable=True),
            SearchField(name="parent_id", type=SearchFieldDataType.String, is_retrievable=True, filterable=True),
            SearchField(name="chunk_number", type=SearchFieldDataType.Int32, is_retrievable=True, filterable=True, sortable=True),
            SearchField(name="chunk_name", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True, facetable=True),
            SearchField(name="source_page_range", type=SearchFieldDataType.String, is_retrievable=True, filterable=True, sortable=True),
            SearchField(name="Client", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True, facetable=True),
            SearchField(name="Project", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True, facetable=True),
            SearchField(name="Module", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True, facetable=True),
            SearchField(name="Source", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True, facetable=True),
            SearchField(name="File", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True),
            SearchField(name="content", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=False, sortable=False, facetable=False),
            SearchField(name="chapter", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True, facetable=True),
            SearchField(name="topic", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True, facetable=True),
            SearchField(name="subtopic", type=SearchFieldDataType.String, is_searchable=True, is_retrievable=True, filterable=True, facetable=True),
            SearchField(name="key_phrases", type=SearchFieldDataType.Collection(SearchFieldDataType.String), is_retrievable=True, filterable=True, facetable=True, is_searchable=True),
            SearchField(name="imageDescription", type=SearchFieldDataType.Collection(SearchFieldDataType.String), is_searchable=True, is_retrievable=True),
            SearchField(name="content_vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                        is_searchable=True, is_retrievable=True,
                        vector_search_dimensions=self.config.VECTOR_DIMENSION,
                        vector_search_profile_name=self.config.VECTOR_SEARCH_PROFILE_NAME),
            ComplexField(name="images", collection=True, fields=[
                SearchField(name="id", type=SearchFieldDataType.String, is_retrievable=True, filterable=True),
                SearchField(name="data", type=SearchFieldDataType.String, is_retrievable=True, is_searchable=False, filterable=False, sortable=False, facetable=False)
            ]),
            ComplexField(name="table_images", collection=True, fields=[
                SearchField(name="id", type=SearchFieldDataType.String, is_retrievable=True, filterable=True),
                SearchField(name="data", type=SearchFieldDataType.String, is_retrievable=True, is_searchable=False, filterable=False, sortable=False, facetable=False)
            ])
        ]

        vector_search_algorithm_dict = {
            "name": self.config.VECTOR_ALGORITHM_CONFIG_NAME,
            "kind": "hnsw",
            "hnsw_parameters": {
                "m": 4,
                "ef_construction": 400,
                "ef_search": 500,
                "metric": "cosine"
            }
        }

        vector_search = VectorSearch(
            profiles=[VectorSearchProfile(name=self.config.VECTOR_SEARCH_PROFILE_NAME, algorithm_configuration_name=self.config.VECTOR_ALGORITHM_CONFIG_NAME)],
            algorithms=[vector_search_algorithm_dict]
        )

        semantic_configuration = SemanticConfiguration(
            name=self.config.SEMANTIC_CONFIG_NAME,
            prioritized_fields=SemanticPrioritizedFields(
                title_field=SemanticField(field_name="topic"),
                content_fields=[SemanticField(field_name="content")],
                keywords_fields=[
                    SemanticField(field_name="key_phrases"),
                    SemanticField(field_name="chapter"),
                    SemanticField(field_name="subtopic")
                ]
            )
        )

        search_index = SearchIndex(
            name=self.config.INDEX_NAME,
            fields=fields,
            vector_search=vector_search,
            semantic_configuration=semantic_configuration
        )

        try:
            self.index_client.create_or_update_index(search_index)
            print(f"✅ Index '{self.config.INDEX_NAME}' created or updated successfully.")
        except Exception as e:
            print(f"❌ Error creating/updating index: {e}")
            raise

    def create_or_update_data_source(self) -> None:
        """Creates or updates the data source."""
        print(f"\n--- Step 2: Creating/updating data source '{self.config.DATASOURCE_NAME}' ---")
        data_source = SearchIndexerDataSourceConnection(
            name=self.config.DATASOURCE_NAME,
            type="azureblob",
            connection_string=self.config.STORAGE_CONNECTION_STRING,
            container={"name": self.config.STORAGE_CONTAINER_NAME}
        )
        self.indexer_client.create_or_update_data_source_connection(data_source)
        print(f"✅ Data Source '{self.config.DATASOURCE_NAME}' created or updated.")

    def create_or_update_skillset(self) -> None:
        """Creates or updates the skillset."""
        print(f"\n--- Step 3: Creating/updating skillset '{self.config.SKILLSET_NAME}' ---")
        skillset_url = f"{self.config.SEARCH_ENDPOINT}/skillsets/{self.config.SKILLSET_NAME}?api-version={self.config.API_VERSION}"
        headers = { "Content-Type": "application/json", "api-key": self.config.SEARCH_API_KEY }
        skillset_payload = {
            "name": self.config.SKILLSET_NAME, "description": "Skillset to generate a vector for content",
            "skills": [
                {
                    "@odata.type": "#Microsoft.Skills.Text.AzureOpenAIEmbeddingSkill",
                    "name": "generate-embeddings-skill",
                    "description": "Generate a vector for the content field",
                    "context": "/document",
                    "inputs": [ { "name": "text", "source": "/document/content" } ],
                    "outputs": [ { "name": "embedding", "targetName": "embedding_vector" } ],
                    "resourceUri": self.config.AZURE_OPENAI_ENDPOINT,
                    "deploymentId": self.config.AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME,
                    "apiKey": self.config.AZURE_OPENAI_API_KEY
                }
            ]
        }
        response = requests.put(skillset_url, headers=headers, data=json.dumps(skillset_payload))

        if response.status_code in (200, 201, 204):
            print(f"✅ Skillset '{self.config.SKILLSET_NAME}' created or updated successfully. (Status: {response.status_code})")
        else:
            print(f"❌ Failed to create/update skillset. Status: {response.status_code}, Response: {response.text}")
            raise Exception("Skillset operation failed")

    def create_or_update_indexer(self) -> None:
        """Creates or updates the indexer."""
        print(f"\n--- Step 4: Creating/updating indexer '{self.config.INDEXER_NAME}' ---")
        indexer = SearchIndexer(
            name=self.config.INDEXER_NAME,
            data_source_name=self.config.DATASOURCE_NAME,
            target_index_name=self.config.INDEX_NAME,
            skillset_name=self.config.SKILLSET_NAME,
            field_mappings=[
                FieldMapping(source_field_name="id", target_field_name="id"),
                FieldMapping(source_field_name="parent_id", target_field_name="parent_id"),
                FieldMapping(source_field_name="chunk_number", target_field_name="chunk_number"),
                FieldMapping(source_field_name="chunk_name", target_field_name="chunk_name"),
                FieldMapping(source_field_name="source_page_range", target_field_name="source_page_range"),
                FieldMapping(source_field_name="Client", target_field_name="Client"),
                FieldMapping(source_field_name="Project", target_field_name="Project"),
                FieldMapping(source_field_name="Module", target_field_name="Module"),
                FieldMapping(source_field_name="Source", target_field_name="Source"),
                FieldMapping(source_field_name="File", target_field_name="File"),
                FieldMapping(source_field_name="content", target_field_name="content"),
                FieldMapping(source_field_name="chapter", target_field_name="chapter"),
                FieldMapping(source_field_name="topic", target_field_name="topic"),
                FieldMapping(source_field_name="subtopic", target_field_name="subtopic"),
                FieldMapping(source_field_name="key_phrases", target_field_name="key_phrases"),
                FieldMapping(source_field_name="imageDescription", target_field_name="imageDescription"),
                FieldMapping(source_field_name="images", target_field_name="images"),
                FieldMapping(source_field_name="table_images", target_field_name="table_images")
            ],
            output_field_mappings=[
                { "source_field_name": "/document/embedding_vector", "target_field_name": "content_vector" }
            ],
            parameters=IndexingParameters(
                max_failed_items=-1,
                configuration={"parsingMode": "json"}
            )
        )
        self.indexer_client.create_or_update_indexer(indexer)
        print(f"✅ Indexer '{self.config.INDEXER_NAME}' created or updated.")

    def _wait_for_indexer_start(self, timeout_seconds: int, polling_interval_seconds: int) -> bool:
        """Waits for the indexer to transition to a 'running' state."""
        start_wait_time = time.time()
        print(f"  Waiting for indexer '{self.config.INDEXER_NAME}' to start (timeout: {timeout_seconds}s)...")
        while time.time() - start_wait_time < timeout_seconds:
            status = self.indexer_client.get_indexer_status(self.config.INDEXER_NAME)
            if status.status == "running":
                print(f"  Indexer '{self.config.INDEXER_NAME}' is now running.")
                return True
            print(f"  Indexer status: {status.status}. Still waiting... (Elapsed: {int(time.time() - start_wait_time)}s)")
            time.sleep(polling_interval_seconds)
        print(f"❌ Indexer '{self.config.INDEXER_NAME}' did not start within the timeout.")
        return False

    def _wait_for_indexer_completion(self, timeout_seconds: int, polling_interval_seconds: int) -> Optional[object]: # Changed return type hint to object
        """
        Polls the indexer status until it completes (success/failure) or times out.
        Returns the last status object on completion, or None on timeout.
        """
        start_wait_time = time.time()
        
        # Track when we first observed 0 items (for the early stop feature)
        zero_item_start_time = None 
        
        # Get the initial status. last_result will contain run-specific details.
        initial_status = self.indexer_client.get_indexer_status(self.config.INDEXER_NAME)
        
        # Track the start time of the specific run we're interested in.
        # If the indexer is already running, use its current run's start_time.
        # Otherwise, it will be None, and we'll wait for a new run to appear.
        expected_run_start_time = None
        if initial_status and initial_status.status == "running" and initial_status.last_result:
            expected_run_start_time = initial_status.last_result.start_time

        print(f"  Monitoring indexer '{self.config.INDEXER_NAME}' run completion (timeout: {timeout_seconds}s)...")
        while time.time() - start_wait_time < timeout_seconds:
            status = self.indexer_client.get_indexer_status(self.config.INDEXER_NAME)
            
            # current_run_details is an IndexerExecutionResult object, if a run occurred.
            current_run_details = status.last_result 

            # Use getattr for robustness against differing SDK versions for item counts
            item_count = getattr(current_run_details, 'item_count', None) or getattr(current_run_details, 'items_processed', 0)
            succeeded_item_count = getattr(current_run_details, 'succeeded_item_count', None) or getattr(current_run_details, 'items_succeeded', 0)
            failed_item_count = getattr(current_run_details, 'failed_item_count', None) or getattr(current_run_details, 'items_failed', 0)
            
            # Case 1: Indexer is currently running
            if status.status == "running":
                if current_run_details and expected_run_start_time is None:
                    # If we just started monitoring and it's already running, capture its start time
                    expected_run_start_time = current_run_details.start_time

                print(f"  Indexer progress: Status={status.status}, Items={item_count}, Succeeded={succeeded_item_count}, Failed={failed_item_count} (Elapsed: {int(time.time() - start_wait_time)}s)")
                
                # Check for "zero item" condition for early stopping
                if item_count == 0:
                    if zero_item_start_time is None:
                        zero_item_start_time = time.time() # Start tracking idle time
                    elif (time.time() - zero_item_start_time) >= self.config.INDEXER_ZERO_ITEM_RUN_THRESHOLD_SECONDS:
                        print(f"  Indexer has consistently reported 0 items for {self.config.INDEXER_ZERO_ITEM_RUN_THRESHOLD_SECONDS} seconds. Stopping indexer...")
                        try:
                            self.indexer_client.stop_indexer(self.config.INDEXER_NAME)
                            print(f"  Indexer '{self.config.INDEXER_NAME}' stop command issued.")
                        except Exception as e:
                            print(f"  Warning: Failed to issue stop command for indexer: {e}")
                        
                        # Wait a bit for the indexer to transition to stopped state
                        time.sleep(self.config.INDEXER_POLLING_INTERVAL_SECONDS) 
                        return self.indexer_client.get_indexer_status(self.config.INDEXER_NAME) # Return final status after attempting stop
                else:
                    # If items are processed, reset the zero_item_start_time
                    zero_item_start_time = None 
            
            # Case 2: Indexer is not running
            elif status.status != "running":
                # If there are valid run details AND it's the run we were tracking (or no specific run was tracked yet)
                if current_run_details and \
                   (expected_run_start_time is None or current_run_details.start_time == expected_run_start_time):
                    print(f"  Indexer run finished. Last run status: {current_run_details.status}")
                    return status # Return the full status object
                
                # If no current_run_details, or it's a different run, keep waiting for a relevant run to appear
                # or for the indexer to start.
                print(f"  Indexer not running. Status: {status.status}. Waiting for run details. (Elapsed: {int(time.time() - start_wait_time)}s)")

            time.sleep(polling_interval_seconds)

        print(f"❌ Indexer '{self.config.INDEXER_NAME}' did not complete within the timeout.")
        return None # Timeout occurred

    def run_indexer(self, reset: bool = False) -> None:
        """Runs the indexer, optionally resetting it, and waits for completion."""
        print(f"\n--- Step 5: Running indexer '{self.config.INDEXER_NAME}' ---")

        if reset:
            print("  FULL REBUILD MODE: Resetting indexer to process all documents")
            try:
                self.indexer_client.reset_indexer(self.config.INDEXER_NAME)
                print("✅ Indexer reset successfully")
            except Exception as e:
                print(f"❌ Error resetting indexer: {e}")
                raise

        try:
            # Initiate the indexer run
            self.indexer_client.run_indexer(self.config.INDEXER_NAME)
            print(f"✅ Indexer run initiated successfully. Monitoring its progress...")
            print("   (Note: It might take a moment for Azure Search to transition to 'running' state.)")

            # Wait for the indexer to start running (optional but good for visibility)
            # Give it 5 polling intervals to visibly start
            if not self._wait_for_indexer_start(
                timeout_seconds=self.config.INDEXER_POLLING_INTERVAL_SECONDS * 5, 
                polling_interval_seconds=self.config.INDEXER_POLLING_INTERVAL_SECONDS
            ):
                print("⚠️ Indexer did not visibly start. Will continue monitoring for completion, but this may indicate an issue or a very quick completion.")


            # Wait for the indexer to complete
            final_status = self._wait_for_indexer_completion(
                timeout_seconds=self.config.INDEXER_RUN_TIMEOUT_MINUTES * 60,
                polling_interval_seconds=self.config.INDEXER_POLLING_INTERVAL_SECONDS
            )

            if final_status and final_status.last_result: # Ensure last_result is present
                last_run_details = final_status.last_result
                
                # Use getattr for robustness against differing SDK versions
                run_status = getattr(last_run_details, 'status', 'unknown')
                start_time = getattr(last_run_details, 'start_time', None)
                end_time = getattr(last_run_details, 'end_time', None)
                total_items = getattr(last_run_details, 'item_count', None) or getattr(last_run_details, 'items_processed', 'N/A')
                succeeded_items = getattr(last_run_details, 'succeeded_item_count', None) or getattr(last_run_details, 'items_succeeded', 'N/A')
                failed_items = getattr(last_run_details, 'failed_item_count', None) or getattr(last_run_details, 'items_failed', 'N/A')
                error_message = getattr(last_run_details, 'error_message', 'N/A') # Ensure this field exists or fallback

                print(f"\n--- Indexer Run Summary for '{self.config.INDEXER_NAME}' ---")
                print(f"  Last Run Status: {run_status}") # Corrected: status of the *run*
                print(f"  Start Time (UTC): {start_time.isoformat() if start_time else 'N/A'}")
                print(f"  End Time (UTC): {end_time.isoformat() if end_time else 'N/A'}")
                print(f"  Total Items: {total_items}")
                print(f"  Succeeded Items: {succeeded_items}")
                print(f"  Failed Items: {failed_items}")
                if error_message != 'N/A': # Only print if there's an actual error message
                    print(f"  Error Message: {error_message}")
                print("------------------------------------------")

                if run_status == "success": # Check the run's status
                    print("🎉 Indexer run completed successfully!")
                else:
                    print(f"⚠️ Indexer run completed with status '{run_status}'. Check logs for more details.")
                    # Optionally, raise an exception here if a failed run should stop the script
                    # raise Exception(f"Indexer run failed with status: {run_status}")
            else:
                print(f"❌ Indexer run did not complete within the specified timeout of {self.config.INDEXER_RUN_TIMEOUT_MINUTES} minutes, or no run details found.")
                # Important: If it times out or no details are found, it's a critical issue, so raise.
                raise Exception("Indexer run timed out or no valid run details retrieved.")

        except Exception as e:
            print(f"\n❌ Failed to run or monitor indexer. Error: {e}")
            raise

    def initialize_and_run_indexer(self):
        """Main function to orchestrate the indexer creation/update and running."""
        if self.config.FULL_REBUILD:
            print("--- FULL REBUILD MODE: Deleting existing components ---")
            # Order matters for deletion: Indexer -> Skillset -> Data Source -> Index
            self.delete_component("indexer", self.config.INDEXER_NAME)
            self.delete_component("skillset", self.config.SKILLSET_NAME)
            self.delete_component("datasource", self.config.DATASOURCE_NAME)
            self.delete_component("index", self.config.INDEX_NAME)
            time.sleep(2) # Give Azure a moment to process deletions

        self.create_or_update_index()
        self.create_or_update_data_source()
        self.create_or_update_skillset()
        self.create_or_update_indexer()
        
        # Add a small delay after creating/updating indexer before running it
        # This helps ensure the service is ready for the run command.
        print("\n--- Waiting for services to propagate changes before running indexer... ---")
        time.sleep(5) 

        self.run_indexer(reset=self.config.FULL_REBUILD) # Reset only for full rebuild

# ==============================================================================
# --- MAIN EXECUTION ---
# ==============================================================================

def main():
    """Main function to orchestrate the indexer operations."""
    print("--- Indexer Rebuild Script Started ---")
    config = IndexerConfig()

    try:
        index_client, indexer_client = initialize_clients(config)
        operations = IndexerOperations(index_client, indexer_client, config)
        operations.initialize_and_run_indexer()

    except Exception as e:
        print(f"❌ An error occurred during the indexer process: {e}")
        import traceback
        traceback.print_exc()
        # Optionally, return a non-zero exit code to signal failure in a CI/CD pipeline
        # sys.exit(1) 
    finally:
        print("--- Indexer Rebuild Script Completed ---")


if __name__ == "__main__":
    main()