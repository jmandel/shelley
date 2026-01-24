---
name: hello-world
description: A simple example skill that prints greetings in different languages.
license: Apache-2.0
compatibility: Works on all platforms with bash
allowed-tools: bash
metadata:
  author: shelley-examples
  version: "1.0"
---

# Hello World Skill

This skill demonstrates how to create a basic Agent Skill for Shelley.

## Purpose

This is a minimal example to show:
- How to structure a SKILL.md file
- Required YAML frontmatter format
- How to write instructions the agent can follow
- Including code examples

## Usage

To greet someone in different languages, use echo commands:

### Available Greetings

1. **English**: "Hello, World!"
2. **Spanish**: "¡Hola, Mundo!"
3. **French**: "Bonjour, le Monde!"
4. **German**: "Hallo, Welt!"
5. **Japanese**: "こんにちは、世界！"
6. **Chinese**: "你好，世界！"

## Example

When asked to demonstrate multilingual greetings:

```bash
echo "English: Hello, World!"
echo "Spanish: ¡Hola, Mundo!"
echo "French: Bonjour, le Monde!"
echo "German: Hallo, Welt!"
echo "Japanese: こんにちは、世界！"
echo "Chinese: 你好，世界！"
```

## Custom Greetings

To greet a specific person or entity:

```bash
NAME="User"
echo "Hello, $NAME!"
echo "¡Hola, $NAME!"
echo "Bonjour, $NAME!"
```

## Notes

- This is a demonstration skill for learning purposes
- Real skills typically provide more complex functionality
- You can use this as a template for creating your own skills
- The skill name must match the directory name (hello-world)

## Installation

To use this skill:

1. Copy the `hello-world` directory to `~/.config/shelley/`
2. Restart Shelley to discover the skill
3. Ask Shelley to use the hello-world skill

```bash
cp -r examples/skills/hello-world ~/.config/shelley/
```
