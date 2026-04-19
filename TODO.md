# Push to prod 
## Backend pre tasks
- [x] Create second storage container
- [x] Backup data into second container
- [x] Run app so its running
- [ ] Verify pipeline is working

## Update front-end to have new schemas and whatever to parse new info
- Test front-end on new data
- Publish new front-end data and stuff

# Can we use the RAG in the front end to source better diffs?
- Maybe add code to claim checker to export semantic search diffs to blob storage (or at least indexes?)
- Then load that and use it as "filter" ("See most relevant / See all")

# Or a more dynamic feature like pulling quotes? Or a worst-of-the-worst (per-company or global)