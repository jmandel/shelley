# Agent Skills in Shelley

Shelley supports the [Agent Skills](https://agentskills.io) open specification, which allows you to extend the agent's capabilities with custom skills packaged as SKILL.md files.

## What are Agent Skills?

Agent Skills are reusable capabilities that can be dynamically discovered and activated by AI agents. Each skill is defined in a standard format that includes:

- **Metadata** (name, description, license, etc.) in YAML frontmatter
- **Instructions** in Markdown that tell the agent how to use the skill

When you ask Shelley to perform a task, it can discover and activate relevant skills automatically.

## Skill File Format

Each skill is a directory containing a `SKILL.md` file. The file must start with YAML frontmatter followed by Markdown instructions:

```markdown
---
name: pdf-processing
description: Extract text and tables from PDF files using pdftotext and tabula-py.
license: MIT
compatibility: Linux, macOS with pdftotext and Python installed
allowed-tools: bash
metadata:
  author: your-name
  version: "1.0"
---

# PDF Processing Skill

This skill enables extraction of text and structured data from PDF files.

## Requirements

- `pdftotext` (from poppler-utils)
- Python 3 with `tabula-py` package

## Usage

1. **Extract text**: Use `pdftotext input.pdf output.txt`
2. **Extract tables**: Use Python with tabula-py to extract tables to CSV

## Example

For a PDF at `/path/to/document.pdf`:
```bash
pdftotext /path/to/document.pdf /tmp/extracted.txt
cat /tmp/extracted.txt
```

## Notes

- Always verify PDF file exists before processing
- Handle errors gracefully if tools are not installed
```

### Required Fields

- **name**: Lowercase, alphanumeric with hyphens only (1-64 chars). Must match the directory name.
- **description**: Clear explanation of what the skill does (max 1024 chars)

### Optional Fields

- **license**: The skill's license (e.g., MIT, Apache-2.0)
- **compatibility**: Platforms or requirements where the skill works
- **allowed-tools**: Tools the skill may use (e.g., bash, python)
- **metadata**: Custom key-value pairs for additional information

## Skill Discovery

Shelley automatically discovers skills from multiple locations:

### 1. User-Level Skills

Skills in your personal configuration directories:

- `~/.config/shelley/` (preferred, XDG convention)
- `~/.shelley/` (legacy location)

Each subdirectory in these locations is checked for a `SKILL.md` file.

**Example structure:**
```
~/.config/shelley/
  pdf-processing/
    SKILL.md
  data-analysis/
    SKILL.md
    scripts/
      analyze.py
```

### 2. Project-Level Skills

Skills anywhere in your project repository:

- Shelley walks the entire git repository tree looking for `SKILL.md` files
- Ignores hidden directories (`.git`, `.github`, etc.)
- Ignores common directories (`node_modules`, `vendor`)

**Example structure:**
```
my-project/
  .git/
  src/
  tools/
    pdf-processor/
      SKILL.md
      converter.py
  docs/
    custom-skill/
      SKILL.md
```

### Skill Name Validation

Skill names must:
- Be 1-64 characters long
- Be lowercase only
- Contain only letters, digits, and hyphens
- Not start or end with a hyphen
- Not contain consecutive hyphens (`--`)
- Match the directory name containing the SKILL.md file

## How Skills Are Activated

1. **At startup**, Shelley discovers all skills from the locations above
2. **Skills are added to the system prompt** in an `<available_skills>` XML block
3. **The agent can see** the name, description, and location of each skill
4. **When a task matches** a skill's description, the agent reads the full SKILL.md file
5. **The agent follows** the instructions in the SKILL.md to accomplish the task

The agent activates skills by reading the SKILL.md file at the location shown in the system prompt. This "lazy loading" approach ensures only relevant skills are loaded into context.

## Creating Your First Skill

Let's create a simple skill to demonstrate:

### 1. Create the skill directory

```bash
mkdir -p ~/.config/shelley/hello-world
cd ~/.config/shelley/hello-world
```

### 2. Create SKILL.md

```bash
cat > SKILL.md << 'EOF'
---
name: hello-world
description: A simple example skill that prints greetings in different languages.
---

# Hello World Skill

This skill demonstrates how to create a basic Agent Skill.

## Usage

To greet someone in different languages:

1. **English**: "Hello, World!"
2. **Spanish**: "¡Hola, Mundo!"
3. **French**: "Bonjour, le Monde!"
4. **Japanese**: "こんにちは、世界！"

## Example

When asked to greet the world in multiple languages:

```bash
echo "Hello, World!"
echo "¡Hola, Mundo!"
echo "Bonjour, le Monde!"
echo "こんにちは、世界！"
```

## Notes

This is a simple demonstration skill. Real skills typically provide more complex capabilities.
EOF
```

### 3. Test the skill

Restart Shelley (if it's running), then ask it:

> "Can you greet me using the hello-world skill?"

The agent will:
1. See the skill in its available skills list
2. Read the SKILL.md file to learn how to use it
3. Execute the instructions to greet you

## Skill Packaging

### Directory Structure

A skill directory can contain:

```
skill-name/
  SKILL.md           # Required: Skill definition
  scripts/           # Optional: Helper scripts
  templates/         # Optional: Template files
  examples/          # Optional: Example files
  README.md          # Optional: Additional docs
```

All file references in SKILL.md should be relative to the skill directory.

### Distribution

Skills can be distributed as:

1. **Git repositories**: Clone directly to `~/.config/shelley/`
2. **Tarballs**: Extract to skill directory
3. **Zip files**: Extract to skill directory (future support planned)

**Example: Installing from a Git repo**

```bash
cd ~/.config/shelley
git clone https://github.com/example/pdf-processing-skill.git pdf-processing
```

## Best Practices

### Skill Design

1. **Clear descriptions**: Make the description searchable and specific
2. **Complete instructions**: Assume the agent has no prior knowledge
3. **Error handling**: Include guidance for common failure cases
4. **Dependencies**: Document all required tools and libraries
5. **Examples**: Provide concrete examples with expected inputs/outputs

### Skill Naming

- Use descriptive names: `pdf-processing` not `pdf`
- Be specific: `csv-data-analyzer` not `analyzer`
- Follow conventions: use hyphens to separate words

### Instructions

- Use clear, numbered steps
- Include code examples with syntax highlighting
- Explain edge cases and limitations
- Keep total length under 5000 tokens to fit in context

### Testing

After creating a skill:

1. Verify the directory name matches the `name` field
2. Check that SKILL.md starts with valid YAML frontmatter
3. Test by asking Shelley to use the skill
4. Confirm the agent can read and follow the instructions

## Examples from the Community

The official Agent Skills repository contains many example skills:

- [anthropics/skills](https://github.com/anthropics/skills) - Community-contributed skills

You can browse these for inspiration or install them directly:

```bash
cd ~/.config/shelley
git clone https://github.com/anthropics/skills
```

## Troubleshooting

### Skill Not Discovered

If your skill isn't being discovered:

1. **Check the location**: Ensure it's in `~/.config/shelley/`, `~/.shelley/`, or your project tree
2. **Check the filename**: Must be exactly `SKILL.md` or `skill.md`
3. **Check the name**: Ensure the directory name matches the `name` field in frontmatter
4. **Check the format**: YAML frontmatter must start with `---` and end with `---`
5. **Restart Shelley**: Skills are discovered at startup

### Skill Validation Errors

Common validation errors:

- **"name and description are required"**: Add both to frontmatter
- **"name must be lowercase"**: Convert name to lowercase
- **"name can only contain letters, digits, and hyphens"**: Remove invalid characters
- **"name cannot start or end with hyphen"**: Adjust the name
- **"description exceeds maximum length"**: Shorten to 1024 chars

### Agent Not Using Skill

If the agent doesn't activate your skill:

1. **Check relevance**: Ensure your task matches the skill's description
2. **Be explicit**: Try saying "use the [skill-name] skill to..."
3. **Check instructions**: Ensure the SKILL.md contains clear, actionable steps
4. **Check dependencies**: Verify all required tools are installed

## Reference

### Specification

Shelley implements the Agent Skills specification from [agentskills.io](https://agentskills.io).

Key components in the codebase:

- `skills/skills.go`: Core implementation of skill discovery and parsing
- `server/system_prompt.go`: Integration with the agent's system prompt
- `skills/skills_test.go`: Test suite demonstrating expected behavior

### Limits

Current implementation limits:

- **Name length**: 1-64 characters
- **Description length**: Max 1024 characters
- **Compatibility field**: Max 500 characters
- **Discovery**: Checks at startup only (restart needed for new skills)

### Future Enhancements

Planned improvements:

- Hot reloading of skills without restart
- Skill marketplace/registry integration
- Native zip file support for skill distribution
- Skill versioning and updates
- Skill dependencies and requirements checking

## Getting Help

If you have questions about Agent Skills in Shelley:

1. Check the [Agent Skills specification](https://agentskills.io/specification)
2. Review the [skills package](./skills/skills.go) implementation
3. Open an issue on the [Shelley repository](https://github.com/boldsoftware/shelley)

For general skill design questions, see the [Agent Skills blog post](https://www.anthropic.com/engineering/equipping-agents-for-the-real-world-with-agent-skills) from Anthropic.
