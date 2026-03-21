# Frontend Design Reference

This document captures the visual design system used in the Bayesian Network Editor so it can be reproduced in other projects.

---

## Layout

The UI is a full-viewport horizontal split: a canvas on the left and a fixed-width resizable panel on the right, separated by a drag handle.

```
┌──────────────────────────────┬──┬─────────────────────┐
│                              │  │  Right Panel        │
│   Canvas / Main Content      │▌ │  (450 px default)   │
│                              │  │                     │
└──────────────────────────────┴──┴─────────────────────┘
```

- **Canvas** (`#network-container`): `flex: 1`, `min-width: 200px`, background `#f8f8f8`
- **Resize handle** (`#resize-handle`): `5px` wide, `cursor: col-resize`, darkens on hover/drag
- **Right panel** (`#panel`): `width: 450px`, `min-width: 200px`, `max-width: 80vw`, `overflow-y: auto`, `overflow-x: hidden`, `padding: 20px`, `gap: 15px` (flex column), `border-left: 1px solid #d0d0d0`

---

## Design Tokens

All values are defined as CSS custom properties on `:root`.

### Colors

| Token | Value | Usage |
|---|---|---|
| `--color-bg-canvas` | `#f8f8f8` | Main canvas background |
| `--color-bg-panel` | `#f0f0f0` | Right panel background |
| `--color-bg-surface` | `#ffffff` | Cards, inputs, tables |
| `--color-border` | `#d0d0d0` | Standard borders |
| `--color-border-light` | `#e4e4e4` | Subtle dividers, table cells |
| `--color-text-primary` | `#1a1a1a` | Body text, labels |
| `--color-text-secondary` | `#555` | De-emphasised text |
| `--color-text-muted` | `#888` | Hints, timestamps, metadata |
| `--color-accent` | `#3b7dd8` | Focus rings, active states |
| `--color-btn-add` | `#cce5ff` | "Add" / create action buttons |
| `--color-btn-success` | `#d4edda` | "Save" / confirm action buttons |
| `--color-btn-danger` | `#f8d7da` | "Delete" / destructive buttons |
| `--color-edge-bg` | `#e8f4ff` | Editor section card background |
| `--color-bar-parent` | `#5b9bd5` | Parent-influence bar fill |
| `--color-bar-child` | `#70ad47` | Child-influence bar fill |

### Shape & Motion

| Token | Value |
|---|---|
| `--radius-sm` | `4px` — inputs, small buttons, table cells |
| `--radius-md` | `6px` — cards/section boxes, tabs container |
| `--transition` | `0.15s ease` — hover states, focus rings |

### Typography

| Token | Value |
|---|---|
| `--font-ui` | `-apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif` |
| `--font-mono` | `'SF Mono', 'Fira Code', 'Consolas', monospace` |

Base font size: `14px`. UI chrome (labels, table text): `13px`. Metadata / hints: `11–12px`.

---

## Form Elements

All `input`, `select`, and `button` elements are `width: 100%`, `box-sizing: border-box`, `padding: 8px`, `font-size: 13px`, `margin-bottom: 5px`.

### Inputs & Selects

- Border: `1px solid #d0d0d0`, radius `--radius-sm`, background `#fff`
- **Focus**: border turns `--color-accent`, box-shadow `0 0 0 2px rgba(59,125,216,0.18)`
- **Readonly**: background `#f4f4f4`, text `--color-text-secondary`, `cursor: default`

### Buttons

Base style: background `#e6e6e6`, border `1px solid rgba(0,0,0,0.14)`, radius `--radius-sm`, `font-weight: 500`.

| State | Effect |
|---|---|
| `:hover` | background `#d9d9d9`, subtle box-shadow |
| `:focus-visible` | box-shadow `0 0 0 2px rgba(59,125,216,0.4)` |
| `:active` | `translateY(1px)`, shadow removed |

#### Button Variants

| Class | Background | Hover | Purpose |
|---|---|---|---|
| `.btn-add` | `#cce5ff` | `#b3d7f7` | Primary create action |
| `.btn-success` | `#d4edda` | `#c3e6cb` | Save / confirm |
| `.btn-danger` | `#f8d7da` | `#f1c2c8` | Delete / destructive |
| `.btn-sm` | inherited | inherited | Compact inline buttons (`width: auto`, `padding: 3px 8px`, `font-size: 12px`) |
| `.btn-action` | — | — | Uniform action size: `padding: 7px 16px`, `font-size: 13px`, `width: auto` |

---

## Panel Components

### Project Bar (`#project-bar`)

Horizontal flex row at the top of the panel, `padding-bottom: 10px`, separated from content below by `border-bottom: 1px solid #d0d0d0`. Contains a label, a `<select>`, a text input, a create button, and a danger delete button. All elements are `width: auto`; the select and input use `flex: 1`.

### Tabs (`.tabs`)

Pill-style segmented control: `background: rgba(0,0,0,0.07)`, `border-radius: --radius-md`, `padding: 3px`, `gap: 2px`.

- Inactive tab: `background: transparent`, text `--color-text-secondary`
- Hover: `background: rgba(0,0,0,0.06)`
- **Active** (`.active`): `background: #fff`, text `--color-text-primary`, `box-shadow: 0 1px 3px rgba(0,0,0,0.15)`

### Editor Section Cards (`.edge-section` / `.node-section`)

Light-blue card wrapping a contextual editor that appears when the user selects an item:

```css
background: #e8f4ff;
border: 1px solid #c2dcf0;
border-radius: 6px;
padding: 8px;
margin-bottom: 10px;
```

Inputs inside get `margin-top: 6px`; buttons get `margin-top: 4px`.

### Save Row (`.save-row`)

Horizontal flex row at the bottom of an editor card: `gap: 8px`, `margin-top: 15px`, `flex-wrap: wrap`, `align-items: center`.

- Primary action (`.btn-success.btn-action`) sits on the left.
- Unsaved-changes badge sits next to it.
- Destructive action (`.btn-danger.btn-action`) is pushed to the far right with `margin-left: auto`.

### Unsaved-Changes Badge (`.dirty-badge`)

Inline pill shown when there are pending changes. Hidden by default (`display:none`).

```css
font-size: 11px;
color: #92400e;
background: #fef3c7;
border: 1px solid #f59e0b;
border-radius: 10px;
padding: 2px 9px;
animation: badge-pulse 1.8s ease-in-out infinite; /* opacity 1 → 0.55 */
```

### Label + Color Picker Row (`.label-row`)

Flex row for a text input paired with a `<input type="color">` swatch:

- Input: `flex: 1`
- Color swatch (`.label-color-picker`): `width: 38px`, `height: 34px`, `padding: 2px`, `flex-shrink: 0`, `border-radius: --radius-sm`, `border: 1px solid #d0d0d0`

---

## Properties Table (`.props-table`)

Compact key/value/type table for arbitrary metadata on nodes and edges.

```css
width: 100%;
border-collapse: collapse;
font-size: 12px;
background: #fff;
border-radius: 4px;
overflow: hidden;
border: 1px solid #e4e4e4;
```

- **Header row**: background `#ebebeb`, `font-size: 11px`, `font-weight: 600`, text `--color-text-secondary`
- **Row hover**: background `#f5f8ff`
- **Key cell** (`.props-td-key`): `font-weight: 500`, truncates at `max-width: 90px`
- **Value cell** (`.props-td-val`): text `--color-text-secondary`, truncates at `max-width: 110px`
- **Type cell** (`.props-td-type`): `font-size: 10px`, text `--color-text-muted`
- **Remove button** (`.prop-remove`): `background: #fdd`, hover `#f5aaaf`, `font-size: 12px`, `padding: 1px 6px`

### Add-Property Row (`#properties-add-row` / `.props-add-row`)

Inline flex row of three inputs (key, value, type select) and a `+` button, all `height: 32px`, `margin-bottom: 0`. Flex sizing: key `flex: 2`, value `flex: 3`, type `flex: 2`.

---

## CPT (Conditional Probability Table)

### Controls Row (`.cpt-controls`)

`display: flex; justify-content: space-between; align-items: center; margin-top: 15px`. Left side: bold label. Right side: auto-fill buttons group (`.cpt-autofill`), flex row with `gap: 4px`.

### Table Container (`#cpt-table-container`)

`overflow-x: auto` — the table scrolls horizontally if the panel is too narrow, preventing overflow out of the card.

### Table

```css
width: 100%;
border-collapse: collapse;
margin-top: 10px;
background: #fff;
font-size: 13px;
text-align: center;
```

- Cell border: `1px solid #e4e4e4`, padding `5px`
- Header cells: background `#e8e8e8`, `font-weight: 500`
- Probability inputs (`.cpt-input`): `width: 60px`, `text-align: center`, `margin: 0`

---

## Ranked Influence Panels (`.rank-section`)

Used in inference mode to visualise marginal probabilities as horizontal bar charts.

```
[node name    ] [████████░░░░░░] [state ]  [pct]
```

- **Row** (`.rank-row`): flex, `gap: 5px`, `margin-bottom: 3px`
- **Name** (`.rank-name`): `width: 110px`, `font-size: 11px`, truncated
- **Bar track** (`.rank-bar-bg`): `flex: 1`, background `#e0e0e0`, `height: 13px`, `border-radius: 2px`
- **Bar fill** (`.rank-bar-fill`): animated width (`transition: width 0.3s ease-out`); parent nodes use `#5b9bd5` (blue), child nodes use `#70ad47` (green)
- **State** (`.rank-state`): `width: 58px`, right-aligned, `font-size: 10px`, muted
- **Percentage** (`.rank-pct`): `width: 36px`, right-aligned, `font-size: 10px`, tabular numbers

---

## Evidence Log (`#evidence-log`)

Monospace text area for displaying set evidence:

```css
font-size: 12px;
line-height: 1.7;
font-family: var(--font-mono);
background: #fff;
border: 1px solid #e4e4e4;
border-radius: 4px;
padding: 8px;
min-height: 36px;
white-space: pre-wrap;
```

---

## Utility Classes

| Class | Purpose |
|---|---|
| `.hint-text` | `font-size: 11px`, italic, `color: #888` — secondary help text |
| `.hint-error` | Same as hint but `color: #c0392b` — inline validation errors |
| `.dirty-badge` | Animated amber pill for unsaved state |
| `.btn-delete-node` | `margin-left: auto` — pushes delete to right edge of save-row |
