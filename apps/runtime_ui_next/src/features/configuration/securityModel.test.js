import test from "node:test";
import assert from "node:assert/strict";

import {
  actorStatusLabel,
  buildCreateActorPayload,
  buildResetPasswordPayload,
  buildSecurityCreateForm,
  buildSecurityPasswordForm,
  buildUpdateActorPayload,
  hasRuntimeAdminRole,
  normalizeActorList,
  roleLabel,
  updateRoleList,
} from "./securityModel.js";

test("security form builders provide safe defaults", () => {
  assert.deepEqual(buildSecurityCreateForm(), {
    username: "",
    email: "",
    display_name: "",
    password: "",
    roles: ["viewer"],
  });
  assert.deepEqual(buildSecurityPasswordForm(), {
    password: "",
    must_rotate_password: false,
  });
});

test("security payload builders normalize form values", () => {
  assert.deepEqual(
    buildCreateActorPayload({
      username: "  analyst-one  ",
      email: "  analyst@example.com ",
      display_name: " Analyst One ",
      password: "  keep-password-as-entered  ",
      roles: [],
    }),
    {
      username: "analyst-one",
      email: "analyst@example.com",
      display_name: "Analyst One",
      password: "  keep-password-as-entered  ",
      roles: ["viewer"],
    },
  );
  assert.deepEqual(buildUpdateActorPayload({ roles: ["admin"], status: "DISABLED" }), {
    roles: ["admin"],
    status: "disabled",
  });
  assert.deepEqual(buildResetPasswordPayload({ password: "new-password", must_rotate_password: 1 }), {
    password: "new-password",
    must_rotate_password: true,
  });
});

test("hasRuntimeAdminRole accepts runtime admin aliases", () => {
  assert.equal(hasRuntimeAdminRole(["viewer"]), false);
  assert.equal(hasRuntimeAdminRole(["admin"]), true);
  assert.equal(hasRuntimeAdminRole(["runtime:admin"]), true);
});

test("updateRoleList toggles roles without allowing an empty list", () => {
  assert.deepEqual(updateRoleList(["viewer"], "analyst"), ["viewer", "analyst"]);
  assert.deepEqual(updateRoleList(["viewer", "analyst"], "viewer"), ["analyst"]);
  assert.deepEqual(updateRoleList(["viewer"], "viewer"), ["viewer"]);
});

test("normalizeActorList prepares actor payloads for the UI", () => {
  const actors = normalizeActorList({
    items: [
      {
        id: "actor-1",
        subject: "analyst-one",
        username: "",
        email: null,
        status: "",
        roles: ["viewer", "analyst"],
      },
    ],
  });

  assert.equal(actors[0].username, "analyst-one");
  assert.equal(actors[0].display_name, "analyst-one");
  assert.equal(actors[0].status, "active");
  assert.deepEqual(actors[0].roles, ["viewer", "analyst"]);
});

test("actor labels are stable", () => {
  assert.equal(actorStatusLabel("disabled"), "Disabled");
  assert.equal(actorStatusLabel("active"), "Active");
  assert.equal(roleLabel("admin"), "Admin");
  assert.equal(roleLabel("custom"), "custom");
});
