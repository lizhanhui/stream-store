# SNAFU Virtual Stack Trace

A lightweight, efficient error handling library for Rust that implements virtual stack traces based on [GreptimeDB's error handling approach](https://greptime.com/blogs/2024-05-07-error-rust). This library combines the power of [SNAFU](https://github.com/shepmaster/snafu) error handling with virtual stack traces to provide meaningful error context without the overhead of system backtraces.

## Table of Contents

- [Motivation](#motivation)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Benefits and Motivations](#benefits-and-motivations)
  - [Basic Example](#basic-example)
  - [Advanced Example](#advanced-example)
- [Do's and Don'ts](#dos-and-donts)
- [How It Works](#how-it-works)
- [Performance](#performance)
- [API Reference](#api-reference)

## Motivation

Traditional error handling in Rust often faces a dilemma:
- **Option 1:** Use system backtraces - long hard to read stack traces only referencing functions and lines
- **Option 2:** Simple error propagation - lacks context about where errors originated

Virtual stack traces provide a third way: capturing meaningful context at each error propagation point with minimal overhead.

## Features

- 🚀 **Lightweight**: Only ~100KB binary overhead vs several MB for system backtraces
- 📍 **Precise Location Tracking**: Automatically captures file, line, and column information
- 🔗 **Error Chain Walking**: Traverses the entire error source chain
- 🎯 **Zero-Cost Abstraction**: Context generation can be postponed until needed
- 🛠️ **Seamless Integration**: Works perfectly with SNAFU error handling
- 📝 **Developer-Friendly**: Automatic Debug implementation with formatted stack traces

## Installation

Add these dependencies to your `Cargo.toml`:

```toml
[dependencies]
snafu = "0.8"
snafu-virtstack = "0.1"
```

## Usage

### Benefits and Motivations

The virtual stack trace approach provides several key advantages:

#### 1. **Performance Efficiency**
Unlike system backtraces that capture the entire call stack (expensive operation), virtual stack traces only record error propagation points. This results in:
- Lower CPU usage during error handling
- Reduced memory footprint
- Smaller binary sizes (100KB vs several MB)

#### 2. **Meaningful Context**
Virtual stack traces capture:
- The exact location where each error was propagated
- Custom error messages at each level
- The complete error chain from root cause to final error

#### 3. **Production-Ready**
- Safe to use in production environments
- No performance penalties in the happy path
- Can be enabled/disabled at runtime if needed

#### 4. **Developer Experience**
- Clear, readable error messages
- Easy to debug error propagation paths
- Automatic integration with existing SNAFU errors

#### 5. **Flexible Error Presentation**
- Detailed traces for developers during debugging
- Simplified messages for end users
- Customizable formatting for different contexts

### Basic Example

```rust
use snafu::prelude::*;
use snafu_virtstack::stack_trace_debug;

#[derive(Snafu)]
#[stack_trace_debug]
pub enum AppError {
    #[snafu(display("Failed to read configuration file"))]
    ConfigRead { source: std::io::Error },

    #[snafu(display("Invalid configuration format"))]
    ConfigParse { source: serde_json::Error },

    #[snafu(display("Database connection failed"))]
    DatabaseConnection { source: DatabaseError },
}

#[derive(Snafu)]
#[stack_trace_debug]
pub enum DatabaseError {
    #[snafu(display("Connection timeout"))]
    Timeout,

    #[snafu(display("Invalid credentials"))]
    InvalidCredentials,
}

fn read_config() -> Result<Config, AppError> {
    let content = std::fs::read_to_string("config.json")
        .context(ConfigReadSnafu)?;

    let config: Config = serde_json::from_str(&content)
        .context(ConfigParseSnafu)?;

    Ok(config)
}

// When an error occurs, you get a detailed virtual stack trace:
// Error: Failed to read configuration file
// Virtual Stack Trace:
//   0: Failed to read configuration file at src/main.rs:45:10
//   1: No such file or directory (os error 2) at src/main.rs:46:15
```

### Advanced Example

```rust
use snafu::prelude::*;
use snafu_virtstack::{stack_trace_debug, VirtualStackTrace};

#[derive(Snafu)]
#[stack_trace_debug]
pub enum ServiceError {
    #[snafu(display("User {id} not found"))]
    UserNotFound { id: u64 },

    #[snafu(display("Failed to query database"))]
    DatabaseQuery { source: DatabaseError },

    #[snafu(display("Failed to serialize response"))]
    Serialization { source: serde_json::Error },

    #[snafu(display("Request validation failed: {reason}"))]
    ValidationFailed { reason: String },
}

#[derive(Snafu)]
#[stack_trace_debug]
pub enum DatabaseError {
    #[snafu(display("Query execution failed"))]
    QueryExecution { source: sqlx::Error },

    #[snafu(display("Connection pool exhausted"))]
    PoolExhausted,

    #[snafu(display("Transaction rolled back"))]
    TransactionRollback,
}

pub struct UserService {
    db: Database,
}

impl UserService {
    pub async fn get_user(&self, id: u64) -> Result<User, ServiceError> {
        // Validation with custom error context
        ensure!(id > 0, ValidationFailedSnafu {
            reason: "User ID must be positive"
        });

        // Database query with error propagation
        let user = self.db
            .query_user(id)
            .await
            .context(DatabaseQuerySnafu)?;

        // Check if user exists
        let user = user.ok_or_else(|| ServiceError::UserNotFound { id })?;

        Ok(user)
    }

    pub async fn get_user_json(&self, id: u64) -> Result<String, ServiceError> {
        let user = self.get_user(id).await?;

        // Serialize with error context
        serde_json::to_string(&user)
            .context(SerializationSnafu)
    }
}

// Error handling in practice
async fn handle_request(service: &UserService, id: u64) {
    match service.get_user_json(id).await {
        Ok(json) => println!("Success: {}", json),
        Err(e) => {
            // For developers: Full virtual stack trace
            eprintln!("{:?}", e);

            // For users: Simple error message
            eprintln!("Error: {}", e);

            // Access the virtual stack programmatically
            let stack = e.virtual_stack();
            for (i, frame) in stack.iter().enumerate() {
                eprintln!("Frame {}: {} at {}:{}",
                    i,
                    frame.message,
                    frame.location.file(),
                    frame.location.line()
                );
            }
        }
    }
}
```

### Boxing Errors

When working with errors that need to be boxed (e.g., when using `Box<dyn std::error::Error + Send + Sync>`), you can use the `.boxed()` method to convert errors before applying context:

```rust
use snafu::prelude::*;
use snafu_virtstack::stack_trace_debug;

#[derive(Snafu)]
#[stack_trace_debug]
pub enum MyError {
    #[snafu(display("Filesystem IO issue: {source}"))]
    FilesystemIoFailure { source: Box<dyn std::error::Error + Send + Sync> },

    #[snafu(display("Configuration error: {source}"))]
    ConfigurationError { source: Box<dyn std::error::Error + Send + Sync> },
}

fn read_config() -> Result<String, MyError> {
    // Box the error before applying context
    let content = std::fs::read_to_string("config.toml")
        .boxed()
        .context(FilesystemIoFailureSnafu)?;
    
    // Parse with another boxed error
    let parsed: Config = toml::from_str(&content)
        .boxed()
        .context(ConfigurationErrorSnafu)?;
    
    Ok(format!("{:?}", parsed))
}
```

The `.boxed()` method is particularly useful when:
- You need to erase the specific error type
- Working with trait objects that require `Send + Sync`
- Dealing with multiple error types that need to be unified
- Creating library APIs that don't want to expose implementation details

## Do's and Don'ts

### ✅ Do's

1. **DO use `#[stack_trace_debug]` on all error enums**
   ```rust
   #[derive(Snafu)]
   #[stack_trace_debug]  // Always add this
   enum MyError { ... }
   ```

2. **DO provide meaningful error messages**
   ```rust
   #[snafu(display("Failed to process user {id}: {reason}"))]
   ProcessingFailed { id: u64, reason: String },
   ```

3. **DO use context when propagating errors**
   ```rust
   operation()
       .context(OperationFailedSnafu { context: "important detail" })?;
   ```

4. **DO separate internal and external errors**
   ```rust
   // Internal error with full details
   #[derive(Snafu)]
   #[stack_trace_debug]
   enum InternalError { ... }

   // External error for API responses
   #[derive(Snafu)]
   enum ApiError { ... }
   ```

5. **DO leverage the error chain**
   ```rust
   // Each level adds context
   low_level_op()
       .context(LowLevelSnafu)?
       .middle_layer()
       .context(MiddleLayerSnafu)?
       .high_level()
       .context(HighLevelSnafu)?;
   ```

6. **DO use `ensure!` for validation**
   ```rust
   ensure!(value > 0, InvalidValueSnafu { value });
   ```

### ❌ Don'ts

1. **DON'T use system backtraces in production**
   ```rust
   // Avoid this in production
   std::env::set_var("RUST_BACKTRACE", "1");
   ```

2. **DON'T ignore error context**
   ```rust
   // Bad: loses context
   operation().map_err(|_| MyError::Generic)?;

   // Good: preserves context
   operation().context(SpecificSnafu)?;
   ```

3. **DON'T create deeply nested error types**
   ```rust
   // Bad: too many levels
   enum Error1 { E2(Error2) }
   enum Error2 { E3(Error3) }
   enum Error3 { E4(Error4) }

   // Good: flat structure with sources
   enum AppError {
       Database { source: DbError },
       Network { source: NetError },
   }
   ```

4. **DON'T expose internal errors directly to users**
   ```rust
   // Bad: exposes implementation details
   return Err(internal_error);

   // Good: map to user-friendly error
   return Err(map_to_external_error(internal_error));
   ```

5. **DON'T forget to handle all error variants**
   ```rust
   // Use exhaustive matching
   match error {
       Error::Variant1 => handle_1(),
       Error::Variant2 => handle_2(),
       // Don't use _ => {} unless necessary
   }
   ```

6. **DON'T mix error handling strategies**
   ```rust
   // Pick one approach and stick to it
   // Either use SNAFU throughout or another error library
   // Don't mix multiple error handling crates
   ```

## How It Works

1. **Proc Macro Magic**: The `#[stack_trace_debug]` attribute automatically implements:
   - `VirtualStackTrace` trait for stack frame collection
   - Custom `Debug` implementation for formatted output

2. **Location Tracking**: Uses Rust's `#[track_caller]` to capture precise locations where errors are propagated

3. **Error Chain Walking**: Automatically traverses the `source()` chain to build complete error context

4. **Zero-Cost Until Needed**: Stack frames are only generated when the error is actually inspected

## API Reference

### Core Traits

#### `VirtualStackTrace`
```rust
pub trait VirtualStackTrace {
    fn virtual_stack(&self) -> Vec<StackFrame>;
}
```

#### `StackFrame`
```rust
pub struct StackFrame {
    pub location: &'static std::panic::Location<'static>,
    pub message: String,
}
```

### Attributes

#### `#[stack_trace_debug]`
Attribute macro that automatically implements virtual stack trace functionality for SNAFU error enums.

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

This project is licensed under the Apache 2.0 License - see the LICENSE file for details.

## Acknowledgments

- Heavily inspired by [GreptimeDB's error handling approach](https://greptime.com/blogs/2024-05-07-error-rust)
- Built on top of the excellent [SNAFU](https://github.com/shepmaster/snafu) error library
- Thanks to the Rust community for amazing error handling discussions

## Related Resources

- [GreptimeDB Error Handling Blog Post](https://greptime.com/blogs/2024-05-07-error-rust)
- [SNAFU Documentation](https://docs.rs/snafu)
- [Rust Error Handling Book](https://doc.rust-lang.org/book/ch09-00-error-handling.html)
