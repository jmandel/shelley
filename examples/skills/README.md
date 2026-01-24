# Example Skills

This directory contains example Agent Skills to demonstrate how to create and use skills with Shelley.

## Available Examples

### 1. hello-world

A minimal skill demonstrating the basic structure of a SKILL.md file. Great for learning the fundamentals.

**Features:**
- Simple multilingual greetings
- Shows required YAML frontmatter
- Demonstrates basic bash commands

**Use case:** Learning how to create skills

### 2. json-formatter

A practical skill for working with JSON data using the `jq` tool.

**Features:**
- Pretty-print JSON
- Validate JSON syntax
- Extract values and fields
- Filter and transform JSON
- Common jq patterns

**Use case:** JSON data processing and formatting

## Using These Examples

### Option 1: Copy to User Directory

To install these skills for personal use:

```bash
# Copy all examples
cp -r examples/skills/* ~/.config/shelley/

# Or copy individual skills
cp -r examples/skills/hello-world ~/.config/shelley/
```

### Option 2: Use from Repository

If you're working within the Shelley repository, these skills will be automatically discovered because Shelley scans the entire git repository tree for SKILL.md files.

### Testing a Skill

After installing a skill:

1. Restart Shelley (if running)
2. Start a conversation
3. Ask Shelley to use the skill:
   - "Use the hello-world skill to greet me"
   - "Use the json-formatter skill to pretty-print this JSON: {...}"

## Creating Your Own Skills

Use these examples as templates:

1. Copy an example directory
2. Rename it to your skill name (lowercase, hyphens only)
3. Edit SKILL.md:
   - Update the `name` field to match the directory name
   - Write a clear `description`
   - Add your instructions in the Markdown body
4. Test with Shelley

For complete documentation, see [SKILLS.md](../../SKILLS.md) in the repository root.

## Contributing Examples

Have a useful skill to share? Consider contributing it:

1. Create your skill following the format shown here
2. Test it thoroughly
3. Add it to this directory
4. Submit a pull request

Good example skills are:
- Well-documented with clear instructions
- Solve a specific, common problem
- Include error handling guidance
- Demonstrate best practices
- Work cross-platform when possible
