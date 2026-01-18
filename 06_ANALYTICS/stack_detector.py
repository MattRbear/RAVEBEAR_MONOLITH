"""
Stack Detector
Finds confluence zones where multiple objects cluster.
Instead of 12 lines, you see 1 stack.
"""

import json
from pathlib import Path
import numpy as np

OBJECTS_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Objects")
OUTPUT_PATH = OBJECTS_DIR / "stacks.json"


def load_objects():
    """Load all object types."""
    objects = []
    
    # Load wicks
    wick_path = OBJECTS_DIR / "wicks.json"
    if wick_path.exists():
        with open(wick_path) as f:
            data = json.load(f)
            for w in data.get('wicks', []):
                objects.append({
                    'type': w['type'],
                    'price': w['price'],
                    'score': w.get('combined_score', 50),
                    'id': w['id']
                })
    
    # Load levels
    level_path = OBJECTS_DIR / "levels.json"
    if level_path.exists():
        with open(level_path) as f:
            data = json.load(f)
            for l in data.get('levels', []):
                objects.append({
                    'type': l['type'],
                    'price': l['price'],
                    'score': l.get('quality_score', 50),
                    'id': l['id']
                })
    
    # Load boxes (use high and low as separate objects)
    box_path = OBJECTS_DIR / "boxes.json"
    if box_path.exists():
        with open(box_path) as f:
            data = json.load(f)
            for b in data.get('boxes', []):
                if b['state'] == 'ACTIVE':
                    objects.append({
                        'type': 'BOX_HIGH',
                        'price': b['high'],
                        'score': b.get('combined_score', 50),
                        'id': b['id'] + '_H'
                    })
                    objects.append({
                        'type': 'BOX_LOW',
                        'price': b['low'],
                        'score': b.get('combined_score', 50),
                        'id': b['id'] + '_L'
                    })
    
    # Load origins
    origin_path = OBJECTS_DIR / "origins.json"
    if origin_path.exists():
        with open(origin_path) as f:
            data = json.load(f)
            for o in data.get('origins', []):
                if o['state'] == 'ACTIVE':
                    objects.append({
                        'type': 'ORIGIN_' + o['direction'],
                        'price': o['zone_mid'],
                        'score': o.get('combined_score', 50),
                        'id': o['id']
                    })
    
    return objects


def find_stacks(objects, cluster_range=50):
    """Find clusters of objects within range."""
    
    if not objects:
        return []
    
    # Sort by price
    objects = sorted(objects, key=lambda x: x['price'])
    
    stacks = []
    used = set()
    
    for i, obj in enumerate(objects):
        if obj['id'] in used:
            continue
        
        # Start a stack
        stack_objects = [obj]
        stack_low = obj['price']
        stack_high = obj['price']
        used.add(obj['id'])
        
        # Find nearby objects
        for j, other in enumerate(objects):
            if other['id'] in used:
                continue
            
            # Check if within range of stack
            if abs(other['price'] - stack_low) <= cluster_range or abs(other['price'] - stack_high) <= cluster_range:
                stack_objects.append(other)
                stack_low = min(stack_low, other['price'])
                stack_high = max(stack_high, other['price'])
                used.add(other['id'])
        
        if len(stack_objects) >= 2:  # Only count as stack if 2+ objects
            # Calculate stack metrics
            types_in_stack = list(set(o['type'] for o in stack_objects))
            avg_score = sum(o['score'] for o in stack_objects) / len(stack_objects)
            
            # Density score: more objects = higher density
            density_score = min(100, len(stack_objects) * 20)
            
            # Diversity bonus: multiple types = more confluence
            diversity_bonus = min(30, len(types_in_stack) * 10)
            
            stacks.append({
                'id': f"STACK_{int(stack_low)}",
                'price_low': float(stack_low),
                'price_high': float(stack_high),
                'price_mid': float((stack_low + stack_high) / 2),
                'object_count': len(stack_objects),
                'types': types_in_stack,
                'type_count': len(types_in_stack),
                'avg_score': round(avg_score, 1),
                'density_score': density_score,
                'confluence_score': round(avg_score * 0.4 + density_score * 0.4 + diversity_bonus, 1),
                'objects': [{'type': o['type'], 'price': o['price'], 'id': o['id']} for o in stack_objects]
            })
    
    return stacks


def main():
    print("Loading all objects...")
    objects = load_objects()
    print(f"Loaded {len(objects)} objects")
    
    if not objects:
        print("No objects found. Run the factory bots first:")
        print("  python level_factory.py")
        print("  python wick_factory.py")
        print("  python box_factory.py")
        print("  python origin_factory.py")
        return
    
    print("Finding confluence stacks...")
    stacks = find_stacks(objects, cluster_range=50)
    
    stacks = sorted(stacks, key=lambda x: x['confluence_score'], reverse=True)
    
    with open(OUTPUT_PATH, 'w') as f:
        json.dump({'stacks': stacks}, f, indent=2)
    
    print(f"\n{'='*70}")
    print(f"  STACK DETECTOR - CONFLUENCE ZONES")
    print(f"{'='*70}")
    print(f"  Total stacks found: {len(stacks)}")
    
    if stacks:
        total_objects_in_stacks = sum(s['object_count'] for s in stacks)
        print(f"  Objects in stacks: {total_objects_in_stacks}")
        print(f"  Objects standalone: {len(objects) - total_objects_in_stacks}")
        
        print(f"\n  TOP 15 CONFLUENCE STACKS:")
        print(f"  {'PRICE RANGE':<25} {'#OBJ':>5} {'TYPES':>5} {'SCORE':>7}")
        print(f"  {'-'*50}")
        
        for s in stacks[:15]:
            price_range = f"${s['price_low']:,.0f} - ${s['price_high']:,.0f}"
            print(f"  {price_range:<25} {s['object_count']:>5} {s['type_count']:>5} {s['confluence_score']:>6.1f}")
    
    print(f"\n  Saved to: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
