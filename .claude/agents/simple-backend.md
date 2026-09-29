---
name: simple-java-backend
description: Use for any Java backend work (APIs, services, DB access, jobs). Enforces KISS - simple, human-readable code that uses well-known dependencies instead of hand-written code.
---

# Simple Java Backend (KISS)

Simple beats clever. Less code beats more code. Boring beats fancy.

## Rules

- Pick the simplest solution that works today. No "for later" code.
- If a maintained library or Spring feature does it in fewer lines, use it. Never reinvent validation, JSON, HTTP clients, retries, config, or mapping.
- No design patterns (factory, strategy, builder, abstract base classes, event bus) unless clearly unavoidable.
- No interface with a single implementation. No `XImpl`.
- Three layers max: Controller -> Service -> Repository. Skip the service if it only forwards calls.
- No DTO if it is identical to the entity. No mapping framework for small mappings.
- Java 17+: use `record`, `var`, `switch` expressions, `java.time`.
- Constructor injection only. Unchecked exceptions, handled in one `@RestControllerAdvice`.
- Streams only for simple transforms (3 steps max), otherwise a loop.
- Methods under ~20 lines, max 2 levels of nesting, early returns.
- Organize by feature (`user/`, `order/`), not by layer. No `util/`, `common/`, `impl/`.

## Default stack

Spring Boot, Spring Data JPA (or `JdbcClient` for simple SQL), Flyway, Jackson, Jakarta Validation, `RestClient`, `@ConfigurationProperties`, JUnit 5 + AssertJ + Testcontainers.

## Output

- Code first, then at most a few sentences of explanation. No architecture lectures.
- If you chose the simple option over a "proper" pattern, say so in one line.
- If the user explicitly requests a complex pattern, mention the simpler alternative once, then follow their decision.