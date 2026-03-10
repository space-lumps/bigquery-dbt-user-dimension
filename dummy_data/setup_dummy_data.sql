-- Create raw.user_core: Primary user profile data
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.user_core` (
  id STRING,
  uuid STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  type STRING,                      -- dropdown: 'E', 'CL', 'IL' or NULL
  race_ethnicity STRING,            -- dropdown: exact values below
  gender STRING,                    -- multi-select dropdown: exact phrases below
  self_describe_gender STRING,
  date_joined TIMESTAMP,
  is_active BOOL,
  is_staff BOOL,
  birthday DATE,
  location_id STRING
)
OPTIONS (
  description = "Primary user profile data including personal details, account status, and demographics"
);

-- Insert dummy rows (mix of types, some duplicates for testing dedup logic if any)
INSERT INTO `dbt-user-dimension-demo.bronze_raw.user_core` (
  id, uuid, first_name, last_name, email, type, race_ethnicity, gender, self_describe_gender,
  date_joined, is_active, is_staff, birthday, location_id
)
VALUES
  ('u001', 'uuid-001', 'Alice', 'Smith', 'alice@example.com', 'IL', 'White', 'Woman', NULL, '2024-01-10 09:00:00 UTC', TRUE, FALSE, '1995-05-20', 'locA'),
  ('u002', 'uuid-002', 'Bob', 'Johnson', 'bob@example.com', 'CL', 'Asian', 'Man', NULL, '2024-02-15 14:30:00 UTC', TRUE, FALSE, '2000-11-03', 'locB'),
  ('u003', 'uuid-003', 'Charlie', 'Lee', 'charlie@example.com', 'E', 'Hispanic', 'Non-binary', 'prefer not to say', '2023-12-01 10:15:00 UTC', TRUE, TRUE, '1988-07-14', 'locC'),
  -- duplicate user id for dedup testing
  ('u001', 'uuid-001', 'Alice', 'Smith', 'alice2@example.com', 'IL', 'White', 'Woman', NULL, '2025-01-05 11:00:00 UTC', TRUE, FALSE, '1995-05-20', NULL),
  ('u005', 'uuid-005', 'Elena', 'Kim', 'elena.k@example.com', 'IL', 'East Asian,Native American or Alaska Native', 'Prefer not to say', NULL, '2024-06-20 13:20:00 UTC', TRUE, FALSE, '1998-09-12', 'locA'),
  ('u006', 'uuid-006', 'Marcus', 'Washington', 'marcus.w@example.com', 'CL', 'Black or African American,Other', 'Man,Prefer to self-describe', 'genderfluid', '2025-02-10 09:45:00 UTC', TRUE, FALSE, '2002-03-28', 'locC'),
  ('u007', 'uuid-007', 'Sofia', 'Rodriguez', 'sofia.r@example.com', 'E', 'Hispanic or Latinx', 'Woman', NULL, '2023-08-05 16:10:00 UTC', TRUE, TRUE, '1985-12-15', 'locB'),
  ('u008', 'uuid-008', 'Test', 'User', 'testuser123@example.com', 'IL', 'White', 'Man', NULL, '2026-01-01 00:00:00 UTC', FALSE, FALSE, '1990-01-01', NULL),
  ('u009', 'uuid-009', 'Jordan', 'Taylor', 'jordan.t@example.com', 'CL', 'Prefer not to say', 'Non-binary', NULL, '2023-03-15 11:30:00 UTC', FALSE, FALSE, '1997-08-07', 'locA'),
  -- Duplicate of u005 with later join date (dedup should pick this one)
  ('u005', 'uuid-005', 'Elena', 'Kim', 'elena.updated@example.com', 'IL', 'East Asian,Native American or Alaska Native', 'Prefer not to say', NULL, '2026-02-20 10:15:00 UTC', TRUE, FALSE, '1998-09-12', 'locA');


-- Create raw.user_join_record: Join action audits
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.user_join_record` (
  user_id STRING,
  action_type STRING,
  sponsor_invite_code_id STRING
)
OPTIONS (
  description = "Audit records of user join actions and events"
);

INSERT INTO `dbt-user-dimension-demo.bronze_raw.user_join_record` (user_id, action_type, sponsor_invite_code_id)
VALUES
  ('u001', 'userjoins', 'invite-abc123'),
  ('u002', 'userjoins', NULL),
  ('u003', 'userjoins', 'invite-edu456'),
  ('u004', 'userjoins', 'invite-xyz789');



-- Create raw.user_site: User-site associations
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.user_site` (
  id STRING,          -- site id
  name STRING,
  sponsor_id STRING
)
OPTIONS (
  description = "Junction table linking users to associated sites"
);

INSERT INTO `dbt-user-dimension-demo.bronze_raw.user_site` (id, name, sponsor_id)
VALUES
  ('site1', 'Main Academy', 'spon001'),
  ('site2', 'West Coast Hub', 'spon002'),
  ('site3', 'Europe Branch', 'spon003');



-- Create raw.user_sponsor: Sponsor master data
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.user_sponsor` (
  id STRING,
  name STRING
)
OPTIONS (
  description = "Sponsor entities that users/sites can be associated with"
);

-- Insert minimal dummy sponsors (enough for joins in int_user_attributions and downstream models)
INSERT INTO `dbt-user-dimension-demo.bronze_raw.user_sponsor` (id, name)
VALUES
  ('spon001', 'Global Education Foundation'),
  ('spon002', 'Tech Learning Partners'),
  ('spon003', 'Community Scholars Network'),
  ('spon004', 'Future Minds Initiative');



-- Classroom entity
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.classroom` (
  id STRING,
  name STRING,
  site_id STRING
)
OPTIONS (description = "Classroom entities (e.g., courses, groups, cohorts)");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.classroom` (id, name, site_id)
VALUES
  ('cls101', 'Intro to Data Science', 'site1'),
  ('cls202', 'Advanced Python', 'site2'),
  ('cls303', 'Statistics for Everyone', 'site1'),
  ('cls404', 'Educator Training', 'site3');



-- Classroom invite codes
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.classroom_invite_code` (
  code STRING,
  classroom_id STRING
)
OPTIONS (description = "Unique codes for joining classrooms");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.classroom_invite_code` (code, classroom_id)
VALUES
  ('INV-ABC123', 'cls101'),
  ('INV-XYZ789', 'cls202'),
  ('INV-EDU456', 'cls303'),
  ('INV-TEACH001', 'cls404');



-- Educator → classroom memberships
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.educator_classroom_membership` (
  user_id STRING,
  classroom_id STRING
)
OPTIONS (description = "Membership records for educators in classrooms");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.educator_classroom_membership` (user_id, classroom_id)
VALUES
  ('u003', 'cls404'),  -- Charlie is an educator
  ('u003', 'cls101');--   - name: educator_invitation_record



-- Educator-sent invitations
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.educator_invitation_record` (
  email STRING,
  classroom_id STRING
)
OPTIONS (description = "Records of invitations sent by educators to potential learners");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.educator_invitation_record` (email, classroom_id)
VALUES
  ('dana@example.com', 'cls202'),
  ('eve@example.com', 'cls303');



-- Learner → classroom memberships
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.learner_classroom_membership` (
  user_id STRING,
  classroom_id STRING
)
OPTIONS (description = "Membership records for learners in classrooms");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.learner_classroom_membership` (user_id, classroom_id)
VALUES
  ('u001', 'cls101'),
  ('u002', 'cls202'),
  ('u001', 'cls303'),  -- Alice joined multiple
  ('u004', 'cls101');



-- location_core with added parents (countries, states, counties)
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.location_core` (
  id STRING,
  display_name STRING,
  long_name STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  slug STRING
)
OPTIONS (description = "Core location master data (IDs, names, coordinates, slugs)");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.location_core` (id, display_name, long_name, latitude, longitude, slug)
VALUES
  -- Cities (type 4)
  ('locA', 'New York', 'New York City, NY, USA', 40.7128, -74.0060, 'new-york-ny'),
  ('locB', 'London', 'London, United Kingdom', 51.5074, -0.1278, 'london-uk'),
  ('locC', 'San Francisco', 'San Francisco, CA, USA', 37.7749, -122.4194, 'san-francisco-ca'),
  -- Counties (type 8, for locA and locC)
  ('county-ny', 'New York County', 'New York County, NY, USA', 40.7831, -73.9712, 'new-york-county-ny'),
  ('county-sf', 'San Francisco County', 'San Francisco County, CA, USA', 37.7749, -122.4194, 'san-francisco-county-ca'),
  -- States/Provinces (type 7)
  ('state-ny', 'New York', 'State of New York, USA', 43.2994, -74.2179, 'new-york-state'),
  ('state-ca', 'California', 'State of California, USA', 36.7783, -119.4179, 'california-state'),
  -- Countries (type 1)
  ('country-usa', 'USA', 'United States of America', 37.0902, -95.7129, 'united-states'),
  ('country-gbr', 'United Kingdom', 'United Kingdom of Great Britain and Northern Ireland', 55.3781, -3.4360, 'united-kingdom');



-- location_address_components: Multiple mappings per city (to county, state, country)
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.location_address_components` (
  from_location_id STRING,  -- Child (e.g., city)
  to_location_id STRING     -- Parent (e.g., county, state, country)
)
OPTIONS (description = "Hierarchical component relationships between locations");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.location_address_components` (from_location_id, to_location_id)
VALUES
  -- locA (New York City) → county, state, country
  ('locA', 'county-ny'),
  ('locA', 'state-ny'),
  ('locA', 'country-usa'),
  -- county-ny → state, country (for full chain)
  ('county-ny', 'state-ny'),
  ('county-ny', 'country-usa'),
  -- state-ny → country
  ('state-ny', 'country-usa'),
  -- locB (London) → country (simplified, no state/county)
  ('locB', 'country-gbr'),
  -- locC (San Francisco) → county, state, country
  ('locC', 'county-sf'),
  ('locC', 'state-ca'),
  ('locC', 'country-usa'),
  -- county-sf → state, country
  ('county-sf', 'state-ca'),
  ('county-sf', 'country-usa'),
  -- state-ca → country
  ('state-ca', 'country-usa');



-- location_type: Assign types to all IDs
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.location_type` (
  location_id STRING,
  locationtype_id INT64   -- 1=country, 3/4=city/admin, 7=state, 8=county
)
OPTIONS (description = "Location type classifications");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.location_type` (location_id, locationtype_id)
VALUES
  ('locA', 4),      -- City
  ('locB', 4),
  ('locC', 4),
  ('county-ny', 8), -- County
  ('county-sf', 8),
  ('state-ny', 7),  -- State
  ('state-ca', 7),
  ('country-usa', 1), -- Country
  ('country-gbr', 1);



-- Sponsor invite codes
CREATE TABLE `dbt-user-dimension-demo.bronze_raw.sponsor_invite_code` (
  id STRING,
  code STRING,
  sponsor_id STRING,
  site_id STRING
)
OPTIONS (description = "Invite codes issued by sponsors for user onboarding");

INSERT INTO `dbt-user-dimension-demo.bronze_raw.sponsor_invite_code` (id, code, sponsor_id, site_id)
VALUES
  ('invite-abc123', 'ABC123', 'spon001', 'site1'),
  ('invite-edu456', 'EDU456', 'spon003', 'site3'),
  ('invite-xyz789', 'XYZ789', 'spon002', 'site2');