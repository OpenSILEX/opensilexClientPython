"""Find target groups for variables."""

import pandas as pd
from typing import List, Dict, Any


def find_target_groups(row: pd.Series, config: Dict[str, Any]) -> List[str]:
    """Find which groups a variable should belong to.
    
    Args:
        row: CSV row containing group information
        config: Configuration dictionary with group settings
        
    Returns:
        List of group URIs for this variable
    """
    group_config = config.get('groups', {})
    group_columns = group_config.get('group_columns', ['Group1', 'Group2'])
    available_groups = group_config.get('available_groups', {})
    default_group = group_config.get('default_group')
    
    target_groups = []
    
    for col in group_columns:
        if col in row and pd.notna(row[col]):
            # Split by multiple separators (semicolon, comma, pipe)
            group_names = str(row[col]).replace(',', ';').replace('|', ';').split(';')
            for gname in group_names:
                gname = gname.strip()
                if gname in available_groups:
                    group_uri = available_groups[gname]
                    if group_uri not in target_groups:
                        target_groups.append(group_uri)
    
    # Fallback to default group if no groups found
    if not target_groups and default_group:
        target_groups.append(default_group)
    
    return target_groups
