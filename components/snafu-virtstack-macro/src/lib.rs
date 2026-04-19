use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, parse_macro_input};

/// Proc macro attribute to automatically generate virtual stack traces for SNAFU errors.
///
/// This attribute automatically implements the [`VirtualStackTrace`] trait and provides
/// a custom [`Debug`] implementation that displays a formatted virtual stack trace.
///
/// See the main [`snafu_virtstack`] crate documentation for comprehensive usage examples
/// and detailed information about virtual stack traces.
///
/// [`VirtualStackTrace`]: snafu_virtstack::VirtualStackTrace
/// [`snafu_virtstack`]: https://docs.rs/snafu_virtstack
#[proc_macro_attribute]
pub fn stack_trace_debug(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Generate the enhanced version with virtual stack trace implementation
    match generate_stack_trace_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate_stack_trace_impl(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Parse the enum to understand its structure
    let _data = match &input.data {
        Data::Enum(data) => data,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "stack_trace_debug can only be applied to enums",
            ));
        }
    };

    // Generate VirtualStackTrace implementation
    let stack_trace_impl =
        generate_virtual_stack_trace_impl(name, &impl_generics, &ty_generics, where_clause)?;

    Ok(quote! {
        // First, emit the original item unchanged
        #input

        // Finally, add the VirtualStackTrace implementation
        #stack_trace_impl
    })
}

fn generate_virtual_stack_trace_impl(
    name: &syn::Ident,
    impl_generics: &syn::ImplGenerics,
    ty_generics: &syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
) -> syn::Result<proc_macro2::TokenStream> {
    Ok(quote! {
        impl #impl_generics snafu_virtstack::VirtualStackTrace for #name #ty_generics #where_clause {
            #[track_caller]
            fn virtual_stack(&self) -> Vec<snafu_virtstack::StackFrame> {
                let mut stack = vec![snafu_virtstack::StackFrame::new(
                    std::panic::Location::caller(),
                    self.to_string(),
                )];

                // Walk the error source chain
                let mut current_error = self as &dyn std::error::Error;
                while let Some(source) = current_error.source() {
                    // Add a simple frame for this source
                    stack.push(snafu_virtstack::StackFrame::new(
                        std::panic::Location::caller(),
                        source.to_string(),
                    ));
                    current_error = source;
                }

                stack
            }
        }

        impl #impl_generics std::fmt::Debug for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                use snafu_virtstack::VirtualStackTrace;

                writeln!(f, "Error: {}", self)?;
                writeln!(f, "Virtual Stack Trace:")?;

                let stack = self.virtual_stack();
                for (i, frame) in stack.iter().enumerate() {
                    writeln!(f, "  {}: {}", i, frame)?;
                }

                Ok(())
            }
        }
    })
}
