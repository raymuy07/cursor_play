#!/usr/bin/env python3
"""
Database Filter Script
Filters records from a database based on specified criteria and exports to JSON.
Designed to be versatile and work with different databases and tables.
"""

import sys
import os
import json
from typing import List, Dict, Any, Optional
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.db_utils import get_db_connection, JOBS_DB

logger = logging.getLogger(__name__)


class DBFilter:
    """
    Versatile database filtering class that can work with any SQLite database.
    Supports multiple filter criteria with OR logic within each filter type
    and AND logic between different filter types.
    """

    def __init__(self, db_path: str, table_name: str = 'jobs'):
        """
        Initialize the database filter.

        Args:
            db_path: Path to the SQLite database file
            table_name: Name of the table to filter (default: 'jobs')
        """
        self.db_path = db_path
        self.table_name = table_name

    def get_unique_values(self, column: str) -> List[str]:
        """
        Get all unique non-null values from a specific column.
        Useful for validating filter values.
        Example:
            >>> filter = DBFilter(JOBS_DB)
            >>> workplace_types = filter.get_unique_values('workplace_type')
            >>> print(workplace_types)  # ['Hybrid', 'Remote', 'On-site']
        """
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                query = f"SELECT DISTINCT {column} FROM {self.table_name} WHERE {column} IS NOT NULL ORDER BY {column}"
                cursor.execute(query)
                results = cursor.fetchall()
                return [row[0] for row in results if row[0]]
        except Exception as e:
            self.logger.error(f"Error fetching unique values for column '{column}': {e}")
            return []

    def _validate_filters(self, filters: Dict[str, List[str]]) -> bool:
        """
        Validate that filter columns exist in the table.
        """
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                # Get table columns
                cursor.execute(f"PRAGMA table_info({self.table_name})")
                columns = [row[1] for row in cursor.fetchall()]

                # Check if all filter keys are valid columns
                for filter_col in filters.keys():
                    if filter_col not in columns:
                        self.logger.error(f"Invalid filter column: '{filter_col}'. Available columns: {columns}")
                        return False
                return True
        except Exception as e:
            self.logger.error(f"Error validating filters: {e}")
            return False

    def _build_filter_query(self, filters: Dict[str, List[str]]) -> tuple:
        """
        Build SQL WHERE clause from filters dictionary.

        Within each filter type: OR logic (e.g., location='Tel Aviv' OR location='Haifa')
        Between filter types: AND logic (e.g., (location filter) AND (employment_type filter))

        For location field: uses LIKE with wildcards to match partial strings
        For other fields: uses exact match (=)

        Args:
            filters: Dictionary where keys are column names and values are lists of filter values
                    Example: {'location': ['Tel Aviv', 'Haifa'], 'employment_type': ['Full-time']}

        Returns:
            Tuple of (where_clause, params_list)
        """
        if not filters:
            return "", []

        where_clauses = []
        params = []

        for column, values in filters.items():
            if not values:  # Skip empty filter lists
                continue

            # Build OR conditions for this filter type
            or_conditions = []

            for value in values:
                if column == 'location':
                    # Use LIKE for location to match partial strings (e.g., "Tel Aviv-Jaffa, IL")
                    or_conditions.append(f"{column} LIKE ?")
                    params.append(f"%{value}%")
                else:
                    # Use exact match for other fields
                    or_conditions.append(f"{column} = ?")
                    params.append(value)

            # Combine OR conditions for this filter type with parentheses
            if or_conditions:
                where_clauses.append(f"({' OR '.join(or_conditions)})")

        # Combine all filter types with AND
        where_clause = " AND ".join(where_clauses) if where_clauses else ""

        return where_clause, params

    def filter_records(self, filters: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        """
        Filter records from the database based on specified criteria.
        Args:
            filters: Dictionary of filter criteria where keys are column names
                    and values are lists of acceptable values
        Returns:
            List of matching records as dictionaries

        Example:
            >>> filter = DBFilter(JOBS_DB)
            >>> results = filter.filter_records({
            ...     'location': ['Tel Aviv', 'Haifa'],
            ...     'employment_type': ['Full-time']
            ... })
            >>> len(results)  # Returns jobs in Tel Aviv OR Haifa AND Full-time
        """
        # Validate filters
        if not self._validate_filters(filters):
            self.logger.error("Filter validation failed")
            return []

        # Build query
        where_clause, params = self._build_filter_query(filters)

        base_query = f"SELECT * FROM {self.table_name}"
        if where_clause:
            query = f"{base_query} WHERE {where_clause} ORDER BY scraped_at DESC"
        else:
            query = f"{base_query} ORDER BY scraped_at DESC"

        self.logger.info(f"Executing query: {query}")
        self.logger.info(f"With parameters: {params}")

        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)

                # Convert rows to dictionaries
                columns = [description[0] for description in cursor.description]
                results = []
                for row in cursor.fetchall():
                    record = dict(zip(columns, row))
                    results.append(record)

                self.logger.info(f"Found {len(results)} matching records")
                return results

        except Exception as e:
            self.logger.error(f"Error executing filter query: {e}")
            return []

    def export_to_json(self, results: List[Dict[str, Any]], output_path: str) -> bool:
        """
        Export filtered results to a JSON file.
        Creates the file if it doesn't exist, overwrites if it does.

        Args:
            results: List of record dictionaries to export
            output_path: Path to the output JSON file

        Returns:
            True if export was successful, False otherwise
        """
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Write to JSON file with pretty formatting
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)

            self.logger.info(f"Successfully exported {len(results)} records to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error exporting to JSON: {e}")
            return False


def main():
    """
    Test implementation of the filter functionality.
    Filters jobs from jobs.db for:
    - Location containing "Tel Aviv"
    - Employment type: "Full-time"
    """
    # Initialize filter with jobs database
    db_filter = DBFilter(db_path=JOBS_DB, table_name='jobs')

    # Get total count before filtering
    with get_db_connection(JOBS_DB) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM jobs")
        total_count = cursor.fetchone()[0]

    logger.info(f"Total jobs in database: {total_count}")

    # Define filters
    filters = {
        'location': ['Tel Aviv']
    }

    logger.info(f"Applying filters: {filters}")

    # Apply filters
    filtered_results = db_filter.filter_records(filters)

    # Output path
    output_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'data',
        'jobs_filtered.json'
    )

    # Export to JSON
    if filtered_results:
        success = db_filter.export_to_json(filtered_results, output_path)
        if success:
            logger.info(f"Summary: {len(filtered_results)}/{total_count} jobs matched filters")
            logger.info(f"Results saved to: {output_path}")
        else:
            logger.error("Failed to export results")
    else:
        logger.warning("No jobs matched the specified filters")

    return filtered_results


if __name__ == "__main__":
    main()

