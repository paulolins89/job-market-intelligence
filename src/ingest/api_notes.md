# hiring.cafe API Notes

## Endpoint
GET https://hiring.cafe/api/search-jobs

## Key Parameters
- `s`: base64-encoded JSON object containing all search filters
- `size`: results per page (40 observed)
- `page`: pagination, 0-indexed
- `sv`: "control" (static)

## The `s` Parameter
Decodes to a JSON object. Key fields:
- `searchQuery`: the search term e.g. "data analyst"
- `locations`: array with country filter (Netherlands)
- `dateFetchedPastNDays`: how far back to look (121 observed)
- `workplaceTypes`: ["Remote", "Hybrid", "Onsite"]
- `commitmentTypes`: ["Full Time", "Part Time", ...]

## Response Structure
- `results`: array of job objects
- Each job has:
  - `id`, `objectID`, `requisition_id`
  - `job_information.title`, `job_information.description` (HTML)
  - `v5_processed_job_data`: pre-extracted structured fields
    - `technical_tools`: list of skills
    - `workplace_cities`, `workplace_type`
    - `yearly_min/max_compensation`
    - `estimated_publish_date`
  - `enriched_company_data`: company info
