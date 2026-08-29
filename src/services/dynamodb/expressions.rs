
//! Expression evaluation for DynamoDB FilterExpression, ConditionExpression,
//! KeyConditionExpression, ProjectionExpression, and UpdateExpression.

use std::collections::HashMap;
use serde_json::Value;

use crate::services::dynamodb::types::AttributeValue;

pub type Item = HashMap<String, AttributeValue>;

/// Substitute expression attribute names (#name → real name).
pub fn resolve_name<'a>(
    name: &'a str,
    expr_names: &'a HashMap<String, String>,
) -> &'a str {
    if let Some(stripped) = name.strip_prefix('#') {
        let key = format!("#{stripped}");
        expr_names.get(&key).map(|s| s.as_str()).unwrap_or(name)
    } else {
        name
    }
}

/// Get a nested attribute value by path (dot-separated, supports [N] for lists).
pub fn get_attr_by_path<'a>(
    item: &'a Item,
    path: &str,
    expr_names: &HashMap<String, String>,
) -> Option<AttributeValue> {
    let parts = split_path(path, expr_names);
    get_nested(item, &parts)
}

fn split_path(path: &str, expr_names: &HashMap<String, String>) -> Vec<PathPart> {
    let mut parts = Vec::new();
    for segment in path.split('.') {
        // Handle list index like attr[0]
        if let Some(bracket) = segment.find('[') {
            let attr_part = &segment[..bracket];
            let resolved = resolve_name(attr_part, expr_names).to_string();
            parts.push(PathPart::Key(resolved));
            let rest = &segment[bracket..];
            // Parse all [N] indices
            let mut r = rest;
            while let Some(close) = r.find(']') {
                let idx_str = &r[1..close];
                if let Ok(idx) = idx_str.parse::<usize>() {
                    parts.push(PathPart::Index(idx));
                }
                r = &r[close + 1..];
            }
        } else {
            let resolved = resolve_name(segment, expr_names).to_string();
            parts.push(PathPart::Key(resolved));
        }
    }
    parts
}

#[derive(Debug)]
enum PathPart {
    Key(String),
    Index(usize),
}

fn get_nested(item: &Item, parts: &[PathPart]) -> Option<AttributeValue> {
    if parts.is_empty() {
        return None;
    }
    let first = match &parts[0] {
        PathPart::Key(k) => item.get(k)?.clone(),
        PathPart::Index(_) => return None,
    };
    get_nested_av(first, &parts[1..])
}

fn get_nested_av(av: AttributeValue, parts: &[PathPart]) -> Option<AttributeValue> {
    if parts.is_empty() {
        return Some(av);
    }
    match (&parts[0], av) {
        (PathPart::Key(k), AttributeValue::M(map)) => {
            let child = map.get(k)?.clone();
            get_nested_av(child, &parts[1..])
        }
        (PathPart::Index(i), AttributeValue::L(list)) => {
            let child = list.get(*i)?.clone();
            get_nested_av(child, &parts[1..])
        }
        _ => None,
    }
}

/// Set a value at a path within an item, creating intermediate maps as needed.
pub fn set_attr_by_path(
    item: &mut Item,
    path: &str,
    expr_names: &HashMap<String, String>,
    value: AttributeValue,
) {
    let parts = split_path(path, expr_names);
    if parts.is_empty() {
        return;
    }
    if parts.len() == 1 {
        if let PathPart::Key(k) = &parts[0] {
            item.insert(k.clone(), value);
        }
        return;
    }
    // For nested paths, get or create the first level
    if let PathPart::Key(k) = &parts[0] {
        let key = k.clone();
        let existing = item.remove(&key).unwrap_or(AttributeValue::M(HashMap::new()));
        let updated = set_nested_av(existing, &parts[1..], value);
        item.insert(key, updated);
    }
}

fn set_nested_av(av: AttributeValue, parts: &[PathPart], value: AttributeValue) -> AttributeValue {
    if parts.is_empty() {
        return value;
    }
    match (&parts[0], av) {
        (PathPart::Key(k), AttributeValue::M(mut map)) => {
            let existing = map.remove(k).unwrap_or(AttributeValue::M(HashMap::new()));
            let updated = set_nested_av(existing, &parts[1..], value);
            map.insert(k.clone(), updated);
            AttributeValue::M(map)
        }
        (PathPart::Index(i), AttributeValue::L(mut list)) => {
            if *i < list.len() {
                let existing = list[*i].clone();
                list[*i] = set_nested_av(existing, &parts[1..], value);
            }
            AttributeValue::L(list)
        }
        _ => value,
    }
}

/// Remove attribute at path from item.
pub fn remove_attr_by_path(
    item: &mut Item,
    path: &str,
    expr_names: &HashMap<String, String>,
) {
    let parts = split_path(path, expr_names);
    if parts.is_empty() {
        return;
    }
    if parts.len() == 1 {
        if let PathPart::Key(k) = &parts[0] {
            item.remove(k);
        }
        return;
    }
    if let PathPart::Key(k) = &parts[0] {
        let key = k.clone();
        if let Some(existing) = item.remove(&key) {
            let updated = remove_nested_av(existing, &parts[1..]);
            item.insert(key, updated);
        }
    }
}

fn remove_nested_av(av: AttributeValue, parts: &[PathPart]) -> AttributeValue {
    if parts.is_empty() {
        return av;
    }
    match (&parts[0], av) {
        (PathPart::Key(k), AttributeValue::M(mut map)) => {
            if parts.len() == 1 {
                map.remove(k);
            } else if let Some(existing) = map.remove(k) {
                let updated = remove_nested_av(existing, &parts[1..]);
                map.insert(k.clone(), updated);
            }
            AttributeValue::M(map)
        }
        (PathPart::Index(i), AttributeValue::L(mut list)) => {
            if parts.len() == 1 {
                if *i < list.len() {
                    list.remove(*i);
                }
            } else if *i < list.len() {
                let existing = list[*i].clone();
                list[*i] = remove_nested_av(existing, &parts[1..]);
            }
            AttributeValue::L(list)
        }
        (_, av) => av,
    }
}

/// Compare two AttributeValues for ordering (returns Option<Ordering>).
fn compare_av(a: &AttributeValue, b: &AttributeValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (AttributeValue::S(sa), AttributeValue::S(sb)) => Some(sa.cmp(sb)),
        (AttributeValue::N(na), AttributeValue::N(nb)) => {
            let fa = na.parse::<f64>().ok()?;
            let fb = nb.parse::<f64>().ok()?;
            fa.partial_cmp(&fb)
        }
        (AttributeValue::B(ba), AttributeValue::B(bb)) => {
            use base64::Engine;
            let da = base64::engine::general_purpose::STANDARD.decode(ba).ok()?;
            let db = base64::engine::general_purpose::STANDARD.decode(bb).ok()?;
            Some(da.cmp(&db))
        }
        _ => None,
    }
}

/// Evaluate a condition/filter expression against an item.
pub fn eval_condition(
    expr: &str,
    item: &Item,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> bool {
    let expr = expr.trim();
    eval_or(expr, item, expr_names, expr_values)
}

fn eval_or(
    expr: &str,
    item: &Item,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> bool {
    // Split on top-level OR
    let parts = split_top_level(expr, " OR ");
    if parts.len() > 1 {
        return parts.iter().any(|p| eval_and(p.trim(), item, expr_names, expr_values));
    }
    eval_and(expr, item, expr_names, expr_values)
}

fn eval_and(
    expr: &str,
    item: &Item,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> bool {
    let parts = split_top_level(expr, " AND ");
    if parts.len() > 1 {
        return parts.iter().all(|p| eval_not(p.trim(), item, expr_names, expr_values));
    }
    eval_not(expr, item, expr_names, expr_values)
}

fn eval_not(
    expr: &str,
    item: &Item,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> bool {
    if let Some(inner) = expr.strip_prefix("NOT ") {
        return !eval_atom(inner.trim(), item, expr_names, expr_values);
    }
    eval_atom(expr, item, expr_names, expr_values)
}

fn eval_atom(
    expr: &str,
    item: &Item,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> bool {
    let expr = expr.trim();

    // Parenthesized group
    if expr.starts_with('(') && expr.ends_with(')') {
        let inner = &expr[1..expr.len() - 1];
        return eval_condition(inner, item, expr_names, expr_values);
    }

    // attribute_exists(path)
    if let Some(inner) = expr.strip_prefix("attribute_exists(").and_then(|s| s.strip_suffix(')')) {
        let val = get_attr_by_path(item, inner.trim(), expr_names);
        return val.is_some();
    }

    // attribute_not_exists(path)
    if let Some(inner) = expr.strip_prefix("attribute_not_exists(").and_then(|s| s.strip_suffix(')')) {
        let val = get_attr_by_path(item, inner.trim(), expr_names);
        return val.is_none();
    }

    // attribute_type(path, :type)
    if let Some(inner) = expr.strip_prefix("attribute_type(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() == 2 {
            let path = parts[0].trim();
            let type_ref = parts[1].trim();
            let expected_type = if type_ref.starts_with(':') {
                expr_values.get(type_ref).and_then(|av| av.as_s().map(str::to_string))
            } else {
                Some(type_ref.trim_matches('"').to_string())
            };
            if let Some(expected) = expected_type {
                if let Some(val) = get_attr_by_path(item, path, expr_names) {
                    return val.type_name() == expected;
                }
            }
        }
        return false;
    }

    // begins_with(path, :val)
    if let Some(inner) = expr.strip_prefix("begins_with(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() == 2 {
            let path = parts[0].trim();
            let val_ref = parts[1].trim();
            let attr = get_attr_by_path(item, path, expr_names);
            let cmp = resolve_value(val_ref, expr_values);
            return match (attr, cmp) {
                (Some(AttributeValue::S(s)), Some(AttributeValue::S(prefix))) => {
                    s.starts_with(prefix.as_str())
                }
                (Some(AttributeValue::B(b)), Some(AttributeValue::B(prefix))) => {
                    b.starts_with(prefix.as_str())
                }
                _ => false,
            };
        }
        return false;
    }

    // contains(path, :val)
    if let Some(inner) = expr.strip_prefix("contains(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() == 2 {
            let path = parts[0].trim();
            let val_ref = parts[1].trim();
            let attr = get_attr_by_path(item, path, expr_names);
            let cmp = resolve_value(val_ref, expr_values);
            return match (attr, cmp) {
                (Some(AttributeValue::S(s)), Some(AttributeValue::S(sub))) => {
                    s.contains(sub.as_str())
                }
                (Some(AttributeValue::SS(set)), Some(AttributeValue::S(val))) => {
                    set.contains(&val)
                }
                (Some(AttributeValue::NS(set)), Some(AttributeValue::N(val))) => {
                    set.contains(&val)
                }
                (Some(AttributeValue::BS(set)), Some(AttributeValue::B(val))) => {
                    set.contains(&val)
                }
                (Some(AttributeValue::L(list)), Some(val)) => list.contains(&val),
                _ => false,
            };
        }
        return false;
    }

    // size(path) op :val
    if let Some(inner) = expr.strip_prefix("size(") {
        if let Some(close) = inner.find(')') {
            let path = &inner[..close];
            let rest = inner[close + 1..].trim();
            let (op, val_ref) = parse_comparison_op(rest);
            let attr = get_attr_by_path(item, path.trim(), expr_names);
            let cmp_val = val_ref.and_then(|v| resolve_value(v, expr_values));
            if let (Some(attr), Some(AttributeValue::N(n))) = (attr, cmp_val) {
                let size = attr_size(&attr) as f64;
                if let Ok(cmp_n) = n.parse::<f64>() {
                    return match op {
                        "=" => (size - cmp_n).abs() < f64::EPSILON,
                        "<>" => (size - cmp_n).abs() >= f64::EPSILON,
                        "<" => size < cmp_n,
                        "<=" => size <= cmp_n,
                        ">" => size > cmp_n,
                        ">=" => size >= cmp_n,
                        _ => false,
                    };
                }
            }
            return false;
        }
    }

    // BETWEEN: path BETWEEN :v1 AND :v2
    if let Some(between_idx) = find_keyword(expr, " BETWEEN ") {
        let path = expr[..between_idx].trim();
        let rest = &expr[between_idx + 9..]; // " BETWEEN " is 9 chars
        if let Some(and_idx) = find_keyword(rest, " AND ") {
            let v1_ref = rest[..and_idx].trim();
            let v2_ref = rest[and_idx + 5..].trim();
            let attr = get_attr_by_path(item, path, expr_names);
            let v1 = resolve_value(v1_ref, expr_values);
            let v2 = resolve_value(v2_ref, expr_values);
            if let (Some(attr), Some(v1), Some(v2)) = (attr, v1, v2) {
                let ge = compare_av(&attr, &v1).map(|o| o != std::cmp::Ordering::Less).unwrap_or(false);
                let le = compare_av(&attr, &v2).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false);
                return ge && le;
            }
        }
        return false;
    }

    // IN: path IN (:v1, :v2, ...)
    if let Some(in_idx) = find_keyword(expr, " IN (") {
        let path = expr[..in_idx].trim();
        let rest = &expr[in_idx + 5..]; // " IN (" is 5 chars
        let list_str = rest.trim_end_matches(')');
        let attr = get_attr_by_path(item, path, expr_names);
        if let Some(attr) = attr {
            let values: Vec<Option<AttributeValue>> = list_str
                .split(',')
                .map(|v| resolve_value(v.trim(), expr_values))
                .collect();
            return values.into_iter().flatten().any(|v| v == attr);
        }
        return false;
    }

    // Comparison operators: path op value
    // Try operators in order from longest to shortest
    for op in &["<>", "<=", ">=", "<", ">", "="] {
        if let Some(idx) = expr.find(op) {
            let path = expr[..idx].trim();
            let val_ref = expr[idx + op.len()..].trim();
            // Avoid matching partial operators
            if *op == "<" && expr[idx..].starts_with("<>") { continue; }
            if *op == "<" && expr[idx..].starts_with("<=") { continue; }
            if *op == ">" && expr[idx..].starts_with(">=") { continue; }
            let attr = get_attr_by_path(item, path, expr_names);
            let cmp_val = resolve_value(val_ref, expr_values);
            if let (Some(attr), Some(cmp_val)) = (attr, cmp_val) {
                return match *op {
                    "=" => attr == cmp_val,
                    "<>" => attr != cmp_val,
                    "<" => compare_av(&attr, &cmp_val)
                        .map(|o| o == std::cmp::Ordering::Less)
                        .unwrap_or(false),
                    "<=" => compare_av(&attr, &cmp_val)
                        .map(|o| o != std::cmp::Ordering::Greater)
                        .unwrap_or(false),
                    ">" => compare_av(&attr, &cmp_val)
                        .map(|o| o == std::cmp::Ordering::Greater)
                        .unwrap_or(false),
                    ">=" => compare_av(&attr, &cmp_val)
                        .map(|o| o != std::cmp::Ordering::Less)
                        .unwrap_or(false),
                    _ => false,
                };
            }
            return false;
        }
    }

    false
}

fn attr_size(av: &AttributeValue) -> usize {
    match av {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => n.len(),
        AttributeValue::B(b) => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.decode(b).map(|d| d.len()).unwrap_or(0)
        }
        AttributeValue::SS(s) => s.iter().map(|x| x.len()).sum(),
        AttributeValue::NS(s) => s.iter().map(|x| x.len()).sum(),
        AttributeValue::BS(s) => s.len(),
        AttributeValue::L(l) => l.len(),
        AttributeValue::M(m) => m.len(),
        _ => 0,
    }
}

fn parse_comparison_op(s: &str) -> (&str, Option<&str>) {
    for op in &["<>", "<=", ">=", "<", ">", "="] {
        if let Some(idx) = s.find(op) {
            return (op, Some(s[idx + op.len()..].trim()));
        }
    }
    ("", None)
}

fn resolve_value(val_ref: &str, expr_values: &HashMap<String, AttributeValue>) -> Option<AttributeValue> {
    let val_ref = val_ref.trim();
    if val_ref.starts_with(':') {
        expr_values.get(val_ref).cloned()
    } else {
        None
    }
}

/// Split expression at top-level occurrences of `sep` (not inside parentheses).
fn split_top_level<'a>(expr: &'a str, sep: &str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut last = 0usize;
    let bytes = expr.as_bytes();
    let sep_bytes = sep.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'(' {
            depth += 1;
            i += 1;
        } else if bytes[i] == b')' {
            if depth > 0 { depth -= 1; }
            i += 1;
        } else if depth == 0 && bytes[i..].starts_with(sep_bytes) {
            parts.push(&expr[last..i]);
            last = i + sep.len();
            i += sep.len();
        } else {
            i += 1;
        }
    }
    parts.push(&expr[last..]);
    parts
}

/// Find a keyword in expression (case-insensitive, top-level only).
fn find_keyword(expr: &str, keyword: &str) -> Option<usize> {
    let upper = expr.to_uppercase();
    let kw_upper = keyword.to_uppercase();
    let mut depth = 0usize;
    let bytes = upper.as_bytes();
    let kw_bytes = kw_upper.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'(' {
            depth += 1;
            i += 1;
        } else if bytes[i] == b')' {
            if depth > 0 { depth -= 1; }
            i += 1;
        } else if depth == 0 && bytes[i..].starts_with(kw_bytes) {
            return Some(i);
        } else {
            i += 1;
        }
    }
    None
}

/// Parse a KeyConditionExpression into (pk_attr, pk_val, sk_condition).
pub struct KeyCondition {
    pub pk_attr: String,
    pub pk_val: AttributeValue,
    pub sk_condition: Option<SkCondition>,
}

pub enum SkCondition {
    Eq(AttributeValue),
    Lt(AttributeValue),
    Le(AttributeValue),
    Gt(AttributeValue),
    Ge(AttributeValue),
    Between(AttributeValue, AttributeValue),
    BeginsWith(AttributeValue),
}

impl SkCondition {
    pub fn matches(&self, sk: &AttributeValue) -> bool {
        match self {
            SkCondition::Eq(v) => sk == v,
            SkCondition::Lt(v) => compare_av(sk, v)
                .map(|o| o == std::cmp::Ordering::Less)
                .unwrap_or(false),
            SkCondition::Le(v) => compare_av(sk, v)
                .map(|o| o != std::cmp::Ordering::Greater)
                .unwrap_or(false),
            SkCondition::Gt(v) => compare_av(sk, v)
                .map(|o| o == std::cmp::Ordering::Greater)
                .unwrap_or(false),
            SkCondition::Ge(v) => compare_av(sk, v)
                .map(|o| o != std::cmp::Ordering::Less)
                .unwrap_or(false),
            SkCondition::Between(v1, v2) => {
                let ge = compare_av(sk, v1).map(|o| o != std::cmp::Ordering::Less).unwrap_or(false);
                let le = compare_av(sk, v2).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false);
                ge && le
            }
            SkCondition::BeginsWith(v) => match (sk, v) {
                (AttributeValue::S(s), AttributeValue::S(prefix)) => s.starts_with(prefix.as_str()),
                _ => false,
            },
        }
    }
}

pub fn parse_key_condition(
    expr: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Option<KeyCondition> {
    // Split on top-level AND (BETWEEN also has AND, so handle carefully)
    // Strategy: split on " AND " at top level
    let parts = split_top_level_kce(expr);

    if parts.is_empty() {
        return None;
    }

    let pk_part = parse_simple_eq(parts[0].trim(), expr_names, expr_values)?;
    let (pk_attr, pk_val) = pk_part;

    let sk_condition = if parts.len() >= 2 {
        let sk_expr = parts[1..].join(" AND ");
        parse_sk_condition(sk_expr.trim(), expr_names, expr_values)
    } else {
        None
    };

    Some(KeyCondition { pk_attr, pk_val, sk_condition })
}

/// Split key condition expression on " AND ", but preserve BETWEEN ... AND ...
fn split_top_level_kce(expr: &str) -> Vec<String> {
    // We need to split on " AND " but not the AND inside BETWEEN
    // Approach: find the first "=" to identify the PK condition, then split after that
    let upper = expr.to_uppercase();

    // Find "BETWEEN" to avoid splitting its AND
    // Simple approach: find first top-level " AND " that is NOT preceded by BETWEEN
    let bytes = upper.as_bytes();
    let and_pattern = b" AND ";
    let between_pattern = b" BETWEEN ";

    // Collect positions of all top-level " AND "
    let mut and_positions = Vec::new();
    let mut i = 0;
    let mut depth = 0;
    while i < bytes.len() {
        if bytes[i] == b'(' { depth += 1; i += 1; }
        else if bytes[i] == b')' { if depth > 0 { depth -= 1; } i += 1; }
        else if depth == 0 && bytes[i..].starts_with(and_pattern) {
            and_positions.push(i);
            i += and_pattern.len();
        } else {
            i += 1;
        }
    }

    if and_positions.is_empty() {
        return vec![expr.to_string()];
    }

    // Determine which AND positions are part of BETWEEN ... AND ...
    // Check if there's a BETWEEN before this AND (without another AND in between)
    let between_positions: Vec<usize> = {
        let mut bp = Vec::new();
        let mut j = 0;
        while j < bytes.len() {
            if bytes[j..].starts_with(between_pattern) {
                bp.push(j);
                j += between_pattern.len();
            } else {
                j += 1;
            }
        }
        bp
    };

    // An AND at position p is "BETWEEN's AND" if there is a BETWEEN between
    // the previous split point and p.
    let mut split_positions = Vec::new();
    let mut last_split = 0;
    for &ap in &and_positions {
        let is_between_and = between_positions.iter().any(|&bp| bp > last_split && bp < ap);
        if !is_between_and {
            split_positions.push(ap);
            last_split = ap + and_pattern.len();
        }
    }

    // Now split on split_positions
    let mut parts = Vec::new();
    let mut start = 0;
    for pos in split_positions {
        parts.push(expr[start..pos].to_string());
        start = pos + 5; // " AND " length
    }
    parts.push(expr[start..].to_string());
    parts
}

fn parse_simple_eq(
    expr: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Option<(String, AttributeValue)> {
    let idx = expr.find('=')?;
    // Make sure it's not != or <= or >=
    if idx > 0 {
        let prev = expr.as_bytes()[idx - 1];
        if prev == b'!' || prev == b'<' || prev == b'>' {
            return None;
        }
    }
    let attr = expr[..idx].trim();
    let val_ref = expr[idx + 1..].trim();
    let attr_name = resolve_name(attr, expr_names).to_string();
    let val = resolve_value(val_ref, expr_values)?;
    Some((attr_name, val))
}

fn parse_sk_condition(
    expr: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Option<SkCondition> {
    let expr = expr.trim();

    // begins_with(#sk, :val)
    if let Some(inner) = expr.strip_prefix("begins_with(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() == 2 {
            let val_ref = parts[1].trim();
            let val = resolve_value(val_ref, expr_values)?;
            return Some(SkCondition::BeginsWith(val));
        }
        return None;
    }

    // BETWEEN :v1 AND :v2
    let upper = expr.to_uppercase();
    if let Some(between_idx) = upper.find(" BETWEEN ") {
        let rest = &expr[between_idx + 9..];
        let upper_rest = rest.to_uppercase();
        if let Some(and_idx) = upper_rest.find(" AND ") {
            let v1_ref = rest[..and_idx].trim();
            let v2_ref = rest[and_idx + 5..].trim();
            let v1 = resolve_value(v1_ref, expr_values)?;
            let v2 = resolve_value(v2_ref, expr_values)?;
            return Some(SkCondition::Between(v1, v2));
        }
    }

    // Comparison operators
    for op in &["<>", "<=", ">=", "<", ">", "="] {
        if let Some(idx) = expr.find(op) {
            let before = expr[..idx].trim();
            // Verify it's just the attr name (after resolving)
            let _attr = resolve_name(before, expr_names);
            let val_ref = expr[idx + op.len()..].trim();
            let val = resolve_value(val_ref, expr_values)?;
            return Some(match *op {
                "=" => SkCondition::Eq(val),
                "<>" => return None, // Not valid for SK condition
                "<" => SkCondition::Lt(val),
                "<=" => SkCondition::Le(val),
                ">" => SkCondition::Gt(val),
                ">=" => SkCondition::Ge(val),
                _ => return None,
            });
        }
    }

    None
}

/// Apply a projection expression to an item, returning only the projected attributes.
pub fn apply_projection(
    item: &Item,
    projection: &str,
    expr_names: &HashMap<String, String>,
) -> Item {
    let mut result = HashMap::new();
    for attr in projection.split(',') {
        let attr = attr.trim();
        let resolved = resolve_name(attr, expr_names);
        if let Some(val) = item.get(resolved) {
            result.insert(resolved.to_string(), val.clone());
        }
    }
    result
}

/// Parse and apply an UpdateExpression to an item.
/// Returns the updated item and (old_item for reference).
pub fn apply_update_expression(
    item: &mut Item,
    expr: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Result<(), String> {
    // Parse clauses: SET ... REMOVE ... ADD ... DELETE ...
    let clauses = parse_update_clauses(expr);

    for (clause_type, clause_body) in &clauses {
        match clause_type.as_str() {
            "SET" => apply_set_clause(item, clause_body, expr_names, expr_values)?,
            "REMOVE" => apply_remove_clause(item, clause_body, expr_names),
            "ADD" => apply_add_clause(item, clause_body, expr_names, expr_values)?,
            "DELETE" => apply_delete_clause(item, clause_body, expr_names, expr_values)?,
            _ => {}
        }
    }

    Ok(())
}

fn parse_update_clauses(expr: &str) -> Vec<(String, String)> {
    let mut clauses = Vec::new();
    let keywords = ["SET", "REMOVE", "ADD", "DELETE"];

    // Find positions of each keyword (case-insensitive)
    let upper = expr.to_uppercase();
    let mut positions: Vec<(usize, &str)> = Vec::new();

    for kw in &keywords {
        let kw_with_space = format!("{} ", kw);
        let mut search_start = 0;
        while let Some(pos) = upper[search_start..].find(kw_with_space.as_str()) {
            let abs_pos = search_start + pos;
            // Make sure it's at word boundary (start or preceded by space)
            if abs_pos == 0 || upper.as_bytes()[abs_pos - 1] == b' ' {
                positions.push((abs_pos, kw));
                search_start = abs_pos + kw_with_space.len();
            } else {
                search_start = abs_pos + 1;
            }
        }
    }

    positions.sort_by_key(|(pos, _)| *pos);

    for i in 0..positions.len() {
        let (start, kw) = positions[i];
        let end = if i + 1 < positions.len() {
            positions[i + 1].0
        } else {
            expr.len()
        };
        let body = expr[start + kw.len() + 1..end].trim().to_string();
        clauses.push((kw.to_string(), body));
    }

    clauses
}

fn apply_set_clause(
    item: &mut Item,
    clause: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Result<(), String> {
    for assignment in split_set_assignments(clause) {
        let assignment = assignment.trim();
        let eq_idx = assignment.find('=')
            .ok_or_else(|| format!("Invalid SET assignment: {assignment}"))?;
        let path = assignment[..eq_idx].trim();
        let rhs = assignment[eq_idx + 1..].trim();

        let value = eval_set_rhs(item, rhs, expr_names, expr_values)?;
        set_attr_by_path(item, path, expr_names, value);
    }
    Ok(())
}

fn split_set_assignments(clause: &str) -> Vec<String> {
    // Split on commas but not inside function calls
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();
    for ch in clause.chars() {
        match ch {
            '(' => { depth += 1; current.push(ch); }
            ')' => { if depth > 0 { depth -= 1; } current.push(ch); }
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current = String::new();
            }
            _ => { current.push(ch); }
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn eval_set_rhs(
    item: &Item,
    rhs: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Result<AttributeValue, String> {
    let rhs = rhs.trim();

    // if_not_exists(path, :val)
    if let Some(inner) = rhs.strip_prefix("if_not_exists(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() == 2 {
            let path = parts[0].trim();
            let val_ref = parts[1].trim();
            let existing = get_attr_by_path(item, path, expr_names);
            if let Some(existing) = existing {
                return Ok(existing);
            }
            return resolve_rhs_value(item, val_ref, expr_names, expr_values)
                .ok_or_else(|| format!("Cannot resolve value: {val_ref}"));
        }
    }

    // list_append(path, :val) or list_append(:val, path)
    if let Some(inner) = rhs.strip_prefix("list_append(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() == 2 {
            let a_ref = parts[0].trim();
            let b_ref = parts[1].trim();
            let a = resolve_rhs_value(item, a_ref, expr_names, expr_values)
                .ok_or_else(|| format!("Cannot resolve: {a_ref}"))?;
            let b = resolve_rhs_value(item, b_ref, expr_names, expr_values)
                .ok_or_else(|| format!("Cannot resolve: {b_ref}"))?;
            return match (a, b) {
                (AttributeValue::L(mut la), AttributeValue::L(lb)) => {
                    la.extend(lb);
                    Ok(AttributeValue::L(la))
                }
                _ => Err("list_append requires list values".to_string()),
            };
        }
    }

    // path + :val or path - :val (arithmetic)
    if let Some(plus_idx) = find_arithmetic_op(rhs, '+') {
        let left = rhs[..plus_idx].trim();
        let right = rhs[plus_idx + 1..].trim();
        let lv = resolve_rhs_value(item, left, expr_names, expr_values)
            .ok_or_else(|| format!("Cannot resolve: {left}"))?;
        let rv = resolve_rhs_value(item, right, expr_names, expr_values)
            .ok_or_else(|| format!("Cannot resolve: {right}"))?;
        return match (&lv, &rv) {
            (AttributeValue::N(a), AttributeValue::N(b)) => {
                let fa: f64 = a.parse().map_err(|_| "Invalid number".to_string())?;
                let fb: f64 = b.parse().map_err(|_| "Invalid number".to_string())?;
                Ok(AttributeValue::N(format_number(fa + fb)))
            }
            _ => Err("+ operator requires number values".to_string()),
        };
    }

    if let Some(minus_idx) = find_arithmetic_op(rhs, '-') {
        let left = rhs[..minus_idx].trim();
        let right = rhs[minus_idx + 1..].trim();
        let lv = resolve_rhs_value(item, left, expr_names, expr_values)
            .ok_or_else(|| format!("Cannot resolve: {left}"))?;
        let rv = resolve_rhs_value(item, right, expr_names, expr_values)
            .ok_or_else(|| format!("Cannot resolve: {right}"))?;
        return match (&lv, &rv) {
            (AttributeValue::N(a), AttributeValue::N(b)) => {
                let fa: f64 = a.parse().map_err(|_| "Invalid number".to_string())?;
                let fb: f64 = b.parse().map_err(|_| "Invalid number".to_string())?;
                Ok(AttributeValue::N(format_number(fa - fb)))
            }
            _ => Err("- operator requires number values".to_string()),
        };
    }

    // Simple value reference
    resolve_rhs_value(item, rhs, expr_names, expr_values)
        .ok_or_else(|| format!("Cannot resolve RHS: {rhs}"))
}

fn find_arithmetic_op(s: &str, op: char) -> Option<usize> {
    // Find + or - that is not inside parentheses
    let mut depth = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => { if depth > 0 { depth -= 1; } }
            c if c == op && depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

fn resolve_rhs_value(
    item: &Item,
    val_ref: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Option<AttributeValue> {
    let val_ref = val_ref.trim();
    if val_ref.starts_with(':') {
        expr_values.get(val_ref).cloned()
    } else {
        // It's a path reference
        get_attr_by_path(item, val_ref, expr_names)
    }
}

fn apply_remove_clause(
    item: &mut Item,
    clause: &str,
    expr_names: &HashMap<String, String>,
) {
    for path in clause.split(',') {
        remove_attr_by_path(item, path.trim(), expr_names);
    }
}

fn apply_add_clause(
    item: &mut Item,
    clause: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Result<(), String> {
    for assignment in clause.split(',') {
        let assignment = assignment.trim();
        let parts: Vec<&str> = assignment.splitn(2, ' ').collect();
        if parts.len() != 2 {
            continue;
        }
        let path = parts[0].trim();
        let val_ref = parts[1].trim();
        let add_val = match expr_values.get(val_ref) {
            Some(v) => v.clone(),
            None => continue,
        };

        let existing = get_attr_by_path(item, path, expr_names);
        let new_val = match (existing, &add_val) {
            (None, v) => v.clone(),
            (Some(AttributeValue::N(existing_n)), AttributeValue::N(add_n)) => {
                let a: f64 = existing_n.parse().unwrap_or(0.0);
                let b: f64 = add_n.parse().unwrap_or(0.0);
                AttributeValue::N(format_number(a + b))
            }
            (Some(AttributeValue::SS(mut existing_set)), AttributeValue::SS(add_set)) => {
                for s in add_set {
                    if !existing_set.contains(s) {
                        existing_set.push(s.clone());
                    }
                }
                AttributeValue::SS(existing_set)
            }
            (Some(AttributeValue::NS(mut existing_set)), AttributeValue::NS(add_set)) => {
                for s in add_set {
                    if !existing_set.contains(s) {
                        existing_set.push(s.clone());
                    }
                }
                AttributeValue::NS(existing_set)
            }
            (Some(AttributeValue::BS(mut existing_set)), AttributeValue::BS(add_set)) => {
                for s in add_set {
                    if !existing_set.contains(s) {
                        existing_set.push(s.clone());
                    }
                }
                AttributeValue::BS(existing_set)
            }
            (Some(existing), _) => existing,
        };
        set_attr_by_path(item, path, expr_names, new_val);
    }
    Ok(())
}

fn apply_delete_clause(
    item: &mut Item,
    clause: &str,
    expr_names: &HashMap<String, String>,
    expr_values: &HashMap<String, AttributeValue>,
) -> Result<(), String> {
    for assignment in clause.split(',') {
        let assignment = assignment.trim();
        let parts: Vec<&str> = assignment.splitn(2, ' ').collect();
        if parts.len() != 2 {
            continue;
        }
        let path = parts[0].trim();
        let val_ref = parts[1].trim();
        let del_val = match expr_values.get(val_ref) {
            Some(v) => v.clone(),
            None => continue,
        };

        let existing = get_attr_by_path(item, path, expr_names);
        if let Some(existing) = existing {
            let new_val = match (existing, &del_val) {
                (AttributeValue::SS(mut set), AttributeValue::SS(del_set)) => {
                    set.retain(|s| !del_set.contains(s));
                    AttributeValue::SS(set)
                }
                (AttributeValue::NS(mut set), AttributeValue::NS(del_set)) => {
                    set.retain(|s| !del_set.contains(s));
                    AttributeValue::NS(set)
                }
                (AttributeValue::BS(mut set), AttributeValue::BS(del_set)) => {
                    set.retain(|s| !del_set.contains(s));
                    AttributeValue::BS(set)
                }
                (existing, _) => existing,
            };
            set_attr_by_path(item, path, expr_names, new_val);
        }
    }
    Ok(())
}

fn format_number(n: f64) -> String {
    // Format as integer if possible, otherwise as float
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        format!("{}", n)
    }
}

/// Compare two attribute values for sort key ordering.
pub fn compare_sort_keys(a: &AttributeValue, b: &AttributeValue) -> std::cmp::Ordering {
    compare_av(a, b).unwrap_or(std::cmp::Ordering::Equal)
}

/// Parse expression attribute values from JSON.
pub fn parse_expr_values(val: &Value) -> HashMap<String, AttributeValue> {
    let mut map = HashMap::new();
    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            if let Ok(av) = serde_json::from_value::<AttributeValue>(v.clone()) {
                map.insert(k.clone(), av);
            }
        }
    }
    map
}

/// Parse expression attribute names from JSON.
pub fn parse_expr_names(val: &Value) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                map.insert(k.clone(), s.to_string());
            }
        }
    }
    map
}
