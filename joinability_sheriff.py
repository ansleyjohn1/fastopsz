class JoinabilitySheriff:
    """
    Simple, mechanical FK-based combination generator.
    No fallbacks. No inference. Just FKs.
    """

    def __init__(self, metadata_store):
        self.metadata_store = metadata_store

    def generate_combinations(self, selected_tables):
        """
        Generate all valid combinations based on FK relationships.

        Args:
            selected_tables: From Schema Scout
            [
                {"connection_id": "conn_123", "table_name": "orders", "score": 0.95},
                {"connection_id": "conn_123", "table_name": "customers", "score": 0.89},
                ...
            ]

        Returns:
            {
                "combinations": [...],
                "metadata": {...}
            }
        """

        # Validate: All tables from same connection
        connection_ids = set(t["connection_id"] for t in selected_tables)

        if len(connection_ids) > 1:
            # Tables from multiple databases - can't join!
            return {
                "combinations": self.generate_singles(selected_tables),
                "metadata": {
                    "error": "Tables from multiple databases. Only singles returned.",
                    "connection_ids": list(connection_ids)
                }
            }

        connection_id = list(connection_ids)[0]
        table_names = [t["table_name"] for t in selected_tables]

        # Get FK map from metadata store
        fk_map = self.metadata_store.get_fk_map(connection_id, table_names)

        # Generate combinations
        combinations = []

        # Step 1: Singles (always)
        combinations.extend(self.generate_singles(selected_tables))

        # Step 2: Pairs (only if FK exists)
        if fk_map:
            combinations.extend(self.generate_pairs(selected_tables, fk_map))

            # Step 3: Chains (only if FK paths exist)
            combinations.extend(self.generate_chains(selected_tables, fk_map))

        # Step 4: Cap at 30
        if len(combinations) > 30:
            combinations = combinations[:30]

        return {
            "combinations": combinations,
            "metadata": {
                "total_combinations": len(combinations),
                "connection_id": connection_id,
                "fk_relationships_found": len(fk_map) if fk_map else 0
            }
        }

    def generate_singles(self, selected_tables):
        """
        Generate single-table combinations.
        Every table by itself is valid.
        """
        return [
            {
                "tables": [table["table_name"]],
                "similarity_score": table["similarity_score"],
                "connection_id": table["connection_id"],
                "join_paths": [],
                "complexity": 1
            }
            for table in selected_tables
        ]

    def generate_pairs(self, selected_tables, fk_map):
        """
        Generate 2-table combinations where FK relationship exists.
        """
        combinations = []
        table_names = {t["table_name"] for t in selected_tables}
        table_scores = {t["table_name"]: t["similarity_score"] for t in selected_tables}
        connection_id = selected_tables[0]["connection_id"]

        # Loop through FK map
        for from_table, fk_targets in fk_map.items():
            # Check if from_table is in selected tables
            if from_table not in table_names:
                continue

            for to_table, fk_info in fk_targets.items():
                # Check if to_table is in selected tables
                if to_table not in table_names:
                    continue

                # Generate combination
                combinations.append({
                    "tables": [from_table, to_table],
                    "similarity_score": (
                        (table_scores[from_table]+
                        table_scores[to_table]) / 2
                    ),
                    "connection_id": connection_id,
                    "join_paths": [
                        {
                            "from_table": from_table,
                            "to_table": to_table,
                            "from_columns": fk_info["from_columns"],
                            "to_columns": fk_info["to_columns"]
                        }
                    ],
                    "complexity": 2
                })

        return sorted(combinations, key=lambda x: x["similarity_score"], reverse=True)

    def generate_chains(self, selected_tables, fk_map):
        """
        Generate 3-table combinations by following FK paths.
        Avoid cycles.
        """
        combinations = []
        table_names = {t["table_name"] for t in selected_tables}
        table_scores = {t["table_name"]: t["similarity_score"] for t in selected_tables}
        connection_id = selected_tables[0]["connection_id"]

        # Loop through all possible starting tables
        for table1, fk_targets_1 in fk_map.items():
            if table1 not in table_names:
                continue

            # Loop through tables connected to table1
            for table2, fk_info_1 in fk_targets_1.items():
                if table2 not in table_names:
                    continue

                # Check if table2 has FKs
                if table2 not in fk_map:
                    continue

                # Loop through tables connected to table2
                for table3, fk_info_2 in fk_map[table2].items():
                    if table3 not in table_names:
                        continue

                    # Avoid cycles: table3 shouldn't be table1
                    if table3 == table1:
                        continue

                    # Generate 3-table chain
                    combinations.append({
                        "tables": [table1, table2, table3],
                        "similarity_score": (
                            (table_scores[table1]+
                            table_scores[table2]+
                            table_scores[table3]) / 3
                        ),
                        "connection_id": connection_id,
                        "join_paths": [
                            {
                                "from_table": table1,
                                "to_table": table2,
                                "from_columns": fk_info_1["from_columns"],
                                "to_columns": fk_info_1["to_columns"]
                            },
                            {
                                "from_table": table2,
                                "to_table": table3,
                                "from_columns": fk_info_2["from_columns"],
                                "to_columns": fk_info_2["to_columns"]
                            }
                        ],
                        "complexity": 3
                    })

        # Limit chains to prevent explosion
        return sorted(combinations, key=lambda x: x["similarity_score"], reverse=True)
     #combinations[:10]