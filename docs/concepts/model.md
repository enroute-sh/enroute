# Data model

Enroute stores Git repositories. Your application defines the product model
around those repositories.

## Repositories

Your application creates each repository with a key it chooses. Enroute treats
the key as opaque and does not store a display name, owner, description,
members, or permissions. Keep those records in your application and map them
to repository keys.

Every Git request requires an `authorize` response from your application. No
default access policy exists. See [repository keys](../reference/the-contract.md#repository-keys).

## Objects and refs

A ref is a name that points to an object ID. `UpdateRefs` changes refs
atomically and does not invoke hooks. It can only target a commit already in
the repository.

Git pushes are the only way to add objects. Enroute does not create merge
commits; a client must push the merge commit before your application moves a
ref to it. Enroute accepts non-fast-forward updates unless your application
rejects them. Use `IsAncestor` to evaluate ancestry.

Enroute supports SHA-1 repositories. Object IDs are 40 lowercase hexadecimal
characters.

## Tenants

A tenant represents one application on a deployment. Operators define tenants
in a configuration file. Each has a permanent ID, hook endpoint URL, and one
or more claimed hostnames.

An exact hostname claim wins over a wildcard claim. Among wildcards, the
longest matching suffix wins. A repository belongs to the tenant that created
it, identified by tenant ID. See [tenant configuration](../operate/configuration.md#tenants).

## Exclusions

Enroute does not authenticate users, store product policy, or create Git
objects. See [Hooks](hooks.md) and [Limitations](../reference/limitations.md).
