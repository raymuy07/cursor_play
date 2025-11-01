#!/usr/bin/env python3
"""
Filter Jobs Script
Filters raw job data based on user preferences (department, location, etc.)
"""

import sys
import os
from typing import List, Dict, Set

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import setup_logging, load_json, save_json


class JobFilter:
    """Filter jobs based on various criteria."""
    
    def __init__(self, preferences: Dict):
        """
        Initialize the job filter with user preferences.
        
        Args:
            preferences: Dictionary containing filtering criteria
        """
        self.preferences = preferences
        self.logger = setup_logging()
    
    def filter_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """
        Filter jobs based on all configured criteria.
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            List of filtered jobs
        """
        filtered_jobs = jobs
        original_count = len(jobs)
        
        # Apply department filter
        if 'departments' in self.preferences:
            filtered_jobs = self._filter_by_department(filtered_jobs)
            self.logger.info(f"After department filter: {len(filtered_jobs)} jobs (removed {original_count - len(filtered_jobs)})")
        
        # Apply location filter (if configured)
        if 'locations' in self.preferences:
            filtered_jobs = self._filter_by_location(filtered_jobs)
            self.logger.info(f"After location filter: {len(filtered_jobs)} jobs")
        
        # Apply experience level filter (if configured)
        if 'experience_levels' in self.preferences:
            filtered_jobs = self._filter_by_experience(filtered_jobs)
            self.logger.info(f"After experience filter: {len(filtered_jobs)} jobs")
        
        # Apply workplace type filter (if configured)
        if 'workplace_types' in self.preferences:
            filtered_jobs = self._filter_by_workplace_type(filtered_jobs)
            self.logger.info(f"After workplace type filter: {len(filtered_jobs)} jobs")
        
        return filtered_jobs
    
    def _filter_by_department(self, jobs: List[Dict]) -> List[Dict]:
        """
        Filter jobs by department.
        
        Supports:
        - include: List of departments to include (whitelist)
        - exclude: List of departments to exclude (blacklist)
        - keywords: List of keywords that must appear in department name
        """
        dept_config = self.preferences.get('departments', {})
        
        # Get configuration
        include_list = set(dept_config.get('include', []))
        exclude_list = set(dept_config.get('exclude', []))
        keywords = [kw.lower() for kw in dept_config.get('keywords', [])]
        
        filtered = []
        
        for job in jobs:
            department = job.get('department', '').strip()
            
            # Skip if no department
            if not department:
                continue
            
            # Check exclusion list (highest priority)
            if department in exclude_list:
                continue
            
            # Check inclusion list (if provided)
            if include_list:
                if department in include_list:
                    filtered.append(job)
                    continue
            
            # Check keywords (if provided)
            if keywords:
                dept_lower = department.lower()
                if any(keyword in dept_lower for keyword in keywords):
                    filtered.append(job)
                    continue
            
            # If no include list and no keywords, include all non-excluded
            if not include_list and not keywords:
                filtered.append(job)
        
        return filtered
    
    def _filter_by_location(self, jobs: List[Dict]) -> List[Dict]:
        """Filter jobs by location."""
        location_config = self.preferences.get('locations', {})
        
        include_list = set(location_config.get('include', []))
        exclude_list = set(location_config.get('exclude', []))
        allow_remote = location_config.get('allow_remote', True)
        
        filtered = []
        
        for job in jobs:
            location = job.get('location', '').strip()
            
            # Check for remote jobs
            if allow_remote and ('remote' in location.lower() or 'hybrid' in job.get('workplace_type', '').lower()):
                filtered.append(job)
                continue
            
            # Check exclusion list
            if any(excluded in location for excluded in exclude_list):
                continue
            
            # Check inclusion list
            if include_list:
                if any(included in location for included in include_list):
                    filtered.append(job)
            else:
                filtered.append(job)
        
        return filtered
    
    def _filter_by_experience(self, jobs: List[Dict]) -> List[Dict]:
        """Filter jobs by experience level."""
        exp_config = self.preferences.get('experience_levels', {})
        
        include_list = [exp.lower() for exp in exp_config.get('include', [])]
        exclude_list = [exp.lower() for exp in exp_config.get('exclude', [])]
        
        if not include_list and not exclude_list:
            return jobs
        
        filtered = []
        
        for job in jobs:
            experience = (job.get('experience_level') or '').strip().lower()
            
            # If no experience level specified, include by default (unless explicitly filtered)
            if not experience:
                if not exclude_list:
                    filtered.append(job)
                continue
            
            # Check exclusion list
            if any(excluded in experience for excluded in exclude_list):
                continue
            
            # Check inclusion list
            if include_list:
                if any(included in experience for included in include_list):
                    filtered.append(job)
            else:
                filtered.append(job)
        
        return filtered
    
    def _filter_by_workplace_type(self, jobs: List[Dict]) -> List[Dict]:
        """Filter jobs by workplace type (On-site, Hybrid, Remote)."""
        workplace_config = self.preferences.get('workplace_types', {})
        
        include_list = [wt.lower() for wt in workplace_config.get('include', [])]
        
        if not include_list:
            return jobs
        
        filtered = []
        
        for job in jobs:
            workplace = (job.get('workplace_type') or '').strip().lower()
            
            if workplace and any(wt in workplace for wt in include_list):
                filtered.append(job)
        
        return filtered
    
    def get_department_statistics(self, jobs: List[Dict]) -> Dict[str, int]:
        """Get statistics about departments in the job list."""
        dept_counts = {}
        
        for job in jobs:
            dept = job.get('department', 'Unknown')
            dept_counts[dept] = dept_counts.get(dept, 0) + 1
        
        return dict(sorted(dept_counts.items(), key=lambda x: x[1], reverse=True))


def load_preferences() -> Dict:
    """
    Load user preferences for job filtering.
    In the future, this could load from a config file or user profile.
    
    Returns:
        Dictionary with filtering preferences
    """
    # For now, return default preferences
    # This can be customized or loaded from a separate config file
    preferences = {
        'departments': {
            # Option 1: Use include list (whitelist specific departments)
            # 'include': [
            #     'Engineering',
            #     'Product Marketing',
            #     'R&D',
            # ],
            
            # Option 2: Use exclude list (blacklist specific departments)
            # 'exclude': [
            #     'Finance',
            #     'Operations',
            #     'Quality Assurance',
            # ],
            
            # Option 3: Use keywords (more flexible)
            'keywords': [
                'engineering',
                'software',
                'tech',
                'development',
            ],
        },
        
        # Location preferences (optional)
        'locations': {
            'include': ['IL'],  # Israel
            'allow_remote': True,
        },
        
        # Experience level preferences (optional)
        # 'experience_levels': {
        #     'include': ['senior', 'intermediate', 'mid'],
        #     # 'exclude': ['intern', 'junior'],
        # },
        
        # Workplace type preferences (optional)
        # 'workplace_types': {
        #     'include': ['hybrid', 'remote'],
        # },
    }
    
    return preferences


if __name__ == "__main__":
    logger = setup_logging()
    
    logger.info("Starting job filtering process")
    print("=" * 60)
    print("Job Filtering Script")
    print("=" * 60)
    
    # Load raw jobs
    raw_jobs_path = 'data/jobs_raw.json'
    filtered_jobs_path = 'data/jobs_filtered.json'
    
    logger.info(f"Loading raw jobs from {raw_jobs_path}")
    raw_jobs = load_json(raw_jobs_path)
    
    if not raw_jobs:
        logger.warning(f"No jobs found in {raw_jobs_path}")
        print(f"No jobs found in {raw_jobs_path}. Please run scrape_jobs.py first.")
        sys.exit(1)
    
    logger.info(f"Loaded {len(raw_jobs)} raw jobs")
    print(f"\nLoaded {len(raw_jobs)} raw jobs from {raw_jobs_path}")
    
    # Display department statistics before filtering
    filter_instance = JobFilter({})
    dept_stats = filter_instance.get_department_statistics(raw_jobs)
    
    print("\nDepartment Statistics (Before Filtering):")
    print("-" * 60)
    for dept, count in dept_stats.items():
        print(f"  {dept}: {count} jobs")
    print("-" * 60)
    
    # Load preferences and filter
    preferences = load_preferences()
    logger.info(f"Loaded filtering preferences: {preferences}")
    
    job_filter = JobFilter(preferences)
    filtered_jobs = job_filter.filter_jobs(raw_jobs)
    
    logger.info(f"Filtering complete: {len(filtered_jobs)} jobs passed filters (removed {len(raw_jobs) - len(filtered_jobs)} jobs)")
    print(f"\nFiltering Results:")
    print(f"  Total raw jobs: {len(raw_jobs)}")
    print(f"  Filtered jobs: {len(filtered_jobs)}")
    print(f"  Removed: {len(raw_jobs) - len(filtered_jobs)}")
    
    # Display department statistics after filtering
    dept_stats_filtered = job_filter.get_department_statistics(filtered_jobs)
    
    print("\nDepartment Statistics (After Filtering):")
    print("-" * 60)
    for dept, count in dept_stats_filtered.items():
        print(f"  {dept}: {count} jobs")
    print("-" * 60)
    
    # Save filtered jobs
    save_json(filtered_jobs, filtered_jobs_path)
    logger.info(f"Successfully saved {len(filtered_jobs)} filtered jobs to {filtered_jobs_path}")
    print(f"\n✓ Saved {len(filtered_jobs)} filtered jobs to {filtered_jobs_path}")
    
    # Display sample of filtered jobs
    if filtered_jobs:
        print("\nSample of Filtered Jobs (First 5):")
        print("-" * 60)
        for i, job in enumerate(filtered_jobs[:5], 1):
            print(f"\n{i}. {job.get('title')}")
            print(f"   Company: {job.get('company_name')}")
            print(f"   Department: {job.get('department')}")
            print(f"   Location: {job.get('location')}")
            print(f"   Experience: {job.get('experience_level', 'N/A')}")
            print(f"   Workplace: {job.get('workplace_type', 'N/A')}")
    
    print("\n" + "=" * 60)
    print("Filtering complete!")
    print("=" * 60)

