# Copyright 2025 Lanka Data Foundation
# SPDX-License-Identifier: Apache-2.0

"""
Graph Query Data Engineering Script
Extracts entity relationship graphs from Neo4j and transforms to visualization format.

Usage:
    python graph_query.py --entity-id "entity-id" [--depth 2] [--output file.json] [--verbose]
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Any

try:
    from neo4j import GraphDatabase
except ImportError:
    print("ERROR: neo4j driver not installed. Run: pip install neo4j")
    sys.exit(1)


class GraphQueryEngine:
    """Query and transform entity relationship graphs from Neo4j"""
    
    def __init__(self, uri: str, user: str, password: str, verbose: bool = False):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.verbose = verbose
        
    def close(self):
        self.driver.close()
        
    def log(self, message: str):
        if self.verbose:
            print(f"[INFO] {message}")
            
    def get_entity_graph(
        self, 
        entity_id: str, 
        depth: int = 2, 
        active_at: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Extract entity graph with nodes and edges for visualization"""
        if depth < 1 or depth > 3:
            raise ValueError("Depth must be between 1 and 3")
            
        if active_at is None:
            active_at = datetime.utcnow().isoformat() + "Z"
            
        self.log(f"Querying graph for entity: {entity_id}")
        self.log(f"Depth: {depth}, Active at: {active_at}, Limit: {limit}")
        
        # Cypher query: variable-depth path with temporal filtering
        cypher_query = f"""
        MATCH path = (root {{Id: $entityId}})-[r*1..{depth}]-(connected)
        WHERE ALL(
            rel IN relationships(path) 
            WHERE rel.Created <= datetime($activeAt) 
            AND (rel.Terminated IS NULL OR rel.Terminated > datetime($activeAt))
        )
        WITH root, relationships(path) as rels, connected
        LIMIT {limit}
        RETURN DISTINCT
            root,
            COLLECT(DISTINCT connected) as connectedNodes,
            [rel IN rels | {{
                relId: rel.Id,
                type: type(rel),
                startNodeId: startNode(rel).Id,
                endNodeId: endNode(rel).Id,
                created: rel.Created,
                terminated: rel.Terminated,
                properties: properties(rel)
            }}] as relationships
        """
        
        self.log(f"Cypher Query:\n{cypher_query}")
        
        with self.driver.session() as session:
            result = session.run(
                cypher_query, 
                entityId=entity_id, 
                activeAt=active_at
            )
            
            record = result.single()
            
            if not record:
                self.log(f"No entity found with ID: {entity_id}")
                return {"nodes": [], "edges": [], "metadata": {"entityId": entity_id, "found": False}}
            
            return self._transform_to_graph_format(record, entity_id, active_at)
    
    def _transform_to_graph_format(
        self, 
        record: Any, 
        root_entity_id: str,
        active_at: str
    ) -> Dict[str, Any]:
        """Transform Neo4j result to visualization format (nodes + edges)"""
        self.log("Transforming data to graph format...")
        
        nodes = []
        edges = []
        node_ids_seen = set()
        edge_ids_seen = set()
        
        def to_str(val):
            return str(val) if val else None
        
        # Extract root node
        root_node = record["root"]
        root_labels = list(root_node.labels) if hasattr(root_node, 'labels') else []
        major_kind = root_labels[0] if root_labels else "Unknown"
        root_props = dict(root_node)
        
        root_data = {
            "id": root_props.get("Id", root_props.get("id", "unknown")),
            "label": root_props.get("Name", root_props.get("name", root_props.get("Id", "Unknown"))),
            "kind": f"{major_kind}.{root_props.get('MinorKind', '')}".rstrip('.'),
            "group": 0,
            "isRoot": True,
            "created": to_str(root_props.get("Created", root_props.get("created"))),
            "terminated": to_str(root_props.get("Terminated", root_props.get("terminated")))
        }
        nodes.append(root_data)
        node_ids_seen.add(root_data["id"])
        
        # Extract connected nodes
        for idx, node in enumerate(record["connectedNodes"]):
            node_props = dict(node) if hasattr(node, '__iter__') else {}
            node_id = node_props.get("Id", node_props.get("id", f"unknown-{idx}"))
            
            if node_id not in node_ids_seen:
                node_labels = list(node.labels) if hasattr(node, 'labels') else []
                major_kind = node_labels[0] if node_labels else "Unknown"
                
                node_data = {
                    "id": node_id,
                    "label": node_props.get("Name", node_props.get("name", node_id)),
                    "kind": f"{major_kind}.{node_props.get('MinorKind', '')}".rstrip('.'),
                    "group": idx + 1,
                    "created": to_str(node_props.get("Created", node_props.get("created"))),
                    "terminated": to_str(node_props.get("Terminated", node_props.get("terminated")))
                }
                nodes.append(node_data)
                node_ids_seen.add(node_id)
        
        # Extract relationships (edges)
        def clean_props(props):
            if not isinstance(props, dict):
                return {}
            return {k: str(v) if hasattr(v, 'iso_format') else v for k, v in props.items()}
        
        for rel_group in record["relationships"]:
            if not isinstance(rel_group, list):
                rel_group = [rel_group]
                
            for rel in rel_group:
                if not rel or not isinstance(rel, dict):
                    continue
                    
                edge_id = f"rel-{rel.get('relId', 'unknown')}"
                
                if edge_id not in edge_ids_seen:
                    is_active = self._is_relationship_active(rel, active_at)
                    
                    edge_data = {
                        "id": edge_id,
                        "from": rel.get("startNodeId", "unknown"),
                        "to": rel.get("endNodeId", "unknown"),
                        "label": rel.get("type", "unknown"),
                        "created": to_str(rel.get("created")),
                        "terminated": to_str(rel.get("terminated")),
                        "active": is_active,
                        "properties": clean_props(rel.get("properties", {}))
                    }
                    edges.append(edge_data)
                    edge_ids_seen.add(edge_id)
        
        self.log(f"Transformed: {len(nodes)} nodes, {len(edges)} edges")
        
        return {
            "nodes": nodes,
            "edges": edges,
            "metadata": {
                "entityId": root_entity_id,
                "found": True,
                "activeAt": active_at,
                "nodeCount": len(nodes),
                "edgeCount": len(edges)
            }
        }
    
    def _is_relationship_active(self, rel: Dict, active_at: str) -> bool:
        """Check if relationship is active at given timestamp"""
        try:
            if not rel or not isinstance(rel, dict):
                return True
                
            active_dt = datetime.fromisoformat(active_at.replace("Z", "+00:00"))
            created = rel.get("created")
            if not created:
                return True
                
            created_dt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
            
            if created_dt > active_dt:
                return False
            
            terminated = rel.get("terminated")
            if terminated:
                terminated_dt = datetime.fromisoformat(str(terminated).replace("Z", "+00:00"))
                return terminated_dt > active_dt
            
            return True
        except Exception as e:
            self.log(f"Warning: Error checking relationship active status: {e}")
            return True


def main():
    parser = argparse.ArgumentParser(
        description="Query entity relationship graphs from Neo4j (Data Engineering Tool)"
    )
    parser.add_argument(
        "--entity-id", 
        required=True, 
        help="Root entity ID to query from"
    )
    parser.add_argument(
        "--depth", 
        type=int, 
        default=2, 
        help="Relationship traversal depth (1-3, default: 2)"
    )
    parser.add_argument(
        "--active-at", 
        help="ISO timestamp for temporal filtering (default: current time)"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        default=100, 
        help="Maximum relationships to return (default: 100)"
    )
    parser.add_argument(
        "--output", 
        help="Output JSON file path (default: print to stdout)"
    )
    parser.add_argument(
        "--neo4j-uri", 
        default=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        help="Neo4j URI (default: bolt://localhost:7687)"
    )
    parser.add_argument(
        "--neo4j-user", 
        default=os.getenv("NEO4J_USER", "neo4j"),
        help="Neo4j username (default: neo4j)"
    )
    parser.add_argument(
        "--neo4j-password", 
        default=os.getenv("NEO4J_PASSWORD", "neo4j123"),
        help="Neo4j password (default: neo4j123)"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Initialize engine
    engine = GraphQueryEngine(
        uri=args.neo4j_uri,
        user=args.neo4j_user,
        password=args.neo4j_password,
        verbose=args.verbose
    )
    
    try:
        # Query graph
        graph_data = engine.get_entity_graph(
            entity_id=args.entity_id,
            depth=args.depth,
            active_at=args.active_at,
            limit=args.limit
        )
        
        # Output results
        json_output = json.dumps(graph_data, indent=2)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(json_output)
            print(f"✓ Graph data exported to: {args.output}")
            print(f"  Nodes: {graph_data['metadata']['nodeCount']}")
            print(f"  Edges: {graph_data['metadata']['edgeCount']}")
        else:
            print(json_output)
            
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        engine.close()


if __name__ == "__main__":
    main()

