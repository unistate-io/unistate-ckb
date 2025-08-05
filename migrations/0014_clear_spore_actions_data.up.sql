-- Temporary migration to clear all spore_actions data
-- This will allow us to reindex with the new output_index based structure

DELETE FROM spore_actions;
