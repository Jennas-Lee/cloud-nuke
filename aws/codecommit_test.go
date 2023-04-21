package aws

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/codecommit"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/telemetry"
	"github.com/gruntwork-io/cloud-nuke/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestListCodeCommitRepositories(t *testing.T) {
	telemetry.InitTelemetry("cloud-nuke", "", "")
	t.Parallel()

	region, err := getRandomRegion()
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	require.NoError(t, err)
	svc := codecommit.New(session)

	// repoName : CodeCommit Repository Name
	repoName := createCodeCommitRepository(t, svc)
	defer deleteCodeCommitRepositoryInTest(t, svc, repoName, true)

	repoNames, err := getAllCodeCommitRepositories(session, time.Now(), config.Config{})
	require.NoError(t, err)
	assert.Contains(t, aws.StringValueSlice(repoNames), aws.StringValue(repoName))
}

func TestTimeFilterExclusionNewlyCreatedCodeCommitRepository(t *testing.T) {
	telemetry.InitTelemetry("cloud-nuke", "", "")
	t.Parallel()

	region, err := getRandomRegion()
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	require.NoError(t, err)
	svc := codecommit.New(session)

	repoName := createCodeCommitRepository(t, svc)
	defer deleteCodeCommitRepositoryInTest(t, svc, repoName, true)

	// Assert CodeCommit Repository is picked up without filters
	repoNamesNewer, err := getAllCodeCommitRepositories(session, time.Now(), config.Config{})
	require.NoError(t, err)
	assert.Contains(t, aws.StringValueSlice(repoNamesNewer), aws.StringValue(repoName))

	// Assert user doesn't appear when we look at users older than 1 Hour
	olderThan := time.Now().Add(-1 * time.Hour)
	repoNamesOlder, err := getAllCodeCommitRepositories(session, olderThan, config.Config{})
	require.NoError(t, err)
	assert.NotContains(t, aws.StringValueSlice(repoNamesOlder), aws.StringValue(repoName))
}

func TestNukeCodeCommitRepositoryOne(t *testing.T) {
	telemetry.InitTelemetry("cloud-nuke", "", "")
	t.Parallel()

	region, err := getRandomRegion()
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	require.NoError(t, err)
	svc := codecommit.New(session)

	// We ignore errors in the delete call here, because it is intended to be a stop gap in case there is a bug in nuke.
	repoName := createCodeCommitRepository(t, svc)
	defer deleteCodeCommitRepositoryInTest(t, svc, repoName, false)
	identifiers := []*string{repoName}

	require.NoError(
		t,
		nukeAllCodeCommitRepositories(session, identifiers),
	)

	// Make sure the CodeCommit Repository is deleted.
	assertCodeCommitRepositoriesDeleted(t, svc, identifiers)
}

func TestNukeCodeCommitRepositoriesMoreThanOne(t *testing.T) {
	telemetry.InitTelemetry("cloud-nuke", "", "")
	t.Parallel()

	region, err := getRandomRegion()
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	require.NoError(t, err)
	svc := codecommit.New(session)

	repoNames := []*string{}
	for i := 0; i < 3; i++ {
		// We ignore errors in the delete call here, because it is intended to be a stop gap in case there is a bug in nuke.
		repoName := createCodeCommitRepository(t, svc)
		defer deleteCodeCommitRepositoryInTest(t, svc, repoName, false)
		repoNames = append(repoNames, repoName)
	}

	require.NoError(
		t,
		nukeAllCodeCommitRepositories(session, repoNames),
	)

	// Make sure the CodeCommit Repositories are deleted.
	assertCodeCommitRepositoriesDeleted(t, svc, repoNames)
}

func TestListCodeCommitApprovalRuleTemplates(t *testing.T) {
	telemetry.InitTelemetry("cloud-nuke", "", "")
	t.Parallel()

	region, err := getRandomRegion()
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	require.NoError(t, err)
	svc := codecommit.New(session)

	// templateName : CodeCommit Approval Rule Template Name
	templateName := createCodeCommitApprovalRuleTemplate(t, svc)
	defer deleteCodeCommitApprovalRuleTemplateInTest(t, svc, templateName, true)

	templateNames, err := getAllCodeCommitApprovalRuleTemplates(session, time.Now(), config.Config{})
	require.NoError(t, err)
	assert.Contains(t, aws.StringValueSlice(templateNames), aws.StringValue(templateName))
}

func TestTimeFilterExclusionNewlyCreatedCodeCommitApprovalRuleTemplate(t *testing.T) {
	telemetry.InitTelemetry("cloud-nuke", "", "")
	t.Parallel()

	region, err := getRandomRegion()
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	require.NoError(t, err)
	svc := codecommit.New(session)

	templateName := createCodeCommitApprovalRuleTemplate(t, svc)
	defer deleteCodeCommitApprovalRuleTemplateInTest(t, svc, templateName, true)

	// Assert CodeCommit Approval Rule Template is picked up without filters
	templateNamesNewer, err := getAllCodeCommitApprovalRuleTemplates(session, time.Now(), config.Config{})
	require.NoError(t, err)
	assert.Contains(t, aws.StringValueSlice(templateNamesNewer), aws.StringValue(templateName))

	// Assert user doesn't appear when we look at users older than 1 Hour
	olderThan := time.Now().Add(-1 * time.Hour)
	templateNamesOlder, err := getAllCodeCommitApprovalRuleTemplates(session, olderThan, config.Config{})
	require.NoError(t, err)
	assert.NotContains(t, aws.StringValueSlice(templateNamesOlder), aws.StringValue(templateName))
}

func TestNukeCodeCommitApprovalRuleTemplateOne(t *testing.T) {
	telemetry.InitTelemetry("cloud-nuke", "", "")
	t.Parallel()

	region, err := getRandomRegion()
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	require.NoError(t, err)
	svc := codecommit.New(session)

	// We ignore errors in the delete call here, because it is intended to be a stop gap in case there is a bug in nuke.
	templateName := createCodeCommitApprovalRuleTemplate(t, svc)
	defer deleteCodeCommitApprovalRuleTemplateInTest(t, svc, templateName, false)
	identifiers := []*string{templateName}

	require.NoError(
		t,
		nukeAllCodeCommitApprovalRuleTemplates(session, identifiers),
	)

	// Make sure the CodeCommit Approval Rule Template is deleted.
	assertCodeCommitApprovalRuleTemplatesDeleted(t, svc, identifiers)
}

func TestNukeCodeCommitApprovalRuleTemplatesMoreThanOne(t *testing.T) {
	telemetry.InitTelemetry("cloud-nuke", "", "")
	t.Parallel()

	region, err := getRandomRegion()
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	require.NoError(t, err)
	svc := codecommit.New(session)

	templateNames := []*string{}
	for i := 0; i < 3; i++ {
		// We ignore errors in the delete call here, because it is intended to be a stop gap in case there is a bug in nuke.
		templateName := createCodeCommitApprovalRuleTemplate(t, svc)
		defer deleteCodeCommitApprovalRuleTemplateInTest(t, svc, templateName, false)
		templateNames = append(templateNames, templateName)
	}

	require.NoError(
		t,
		nukeAllCodeCommitApprovalRuleTemplates(session, templateNames),
	)

	// Make sure the CodeCommit Approval Rule Templates are deleted.
	assertCodeCommitApprovalRuleTemplatesDeleted(t, svc, templateNames)
}

// Helpers functions for driving the CodeCommit tests

// createCodeCommitRepository will create a new CodeCommit Repository.
func createCodeCommitRepository(t *testing.T, svc *codecommit.CodeCommit) *string {
	uniqueID := util.UniqueID()
	name := fmt.Sprintf("cloud-nuke-testing-%s", uniqueID)

	_, err := svc.CreateRepository(&codecommit.CreateRepositoryInput{
		RepositoryName:        aws.String(name),
		RepositoryDescription: aws.String(name),
	})
	require.NoError(t, err)

	// Verify that the repository is generated well
	resp, err := svc.GetRepository(&codecommit.GetRepositoryInput{
		RepositoryName: aws.String(name),
	})
	require.NoError(t, err)
	if resp == nil {
		t.Fatalf("Error creating Repository %s", name)
	}
	// And an arbitrary sleep to account for eventual consistency
	time.Sleep(15 * time.Second)
	return &name
}

// deleteCodeCommitRepositoryInTest is a function to delete the given CodeCommit Repository.
func deleteCodeCommitRepositoryInTest(t *testing.T, svc *codecommit.CodeCommit, name *string, checkErr bool) {
	input := &codecommit.DeleteRepositoryInput{RepositoryName: name}
	_, err := svc.DeleteRepository(input)
	if checkErr {
		require.NoError(t, err)
	}
}

func assertCodeCommitRepositoriesDeleted(t *testing.T, svc *codecommit.CodeCommit, identifiers []*string) {
	for _, name := range identifiers {
		resp, err := svc.GetRepository(&codecommit.GetRepositoryInput{RepositoryName: name})
		require.ErrorContainsf(t, err, "RepositoryDoesNotExistException", err.Error())
		if resp.RepositoryMetadata != nil {
			t.Fatalf("Repository %s is not deleted", aws.StringValue(name))
		}
	}
}

// createCodeCommitApprovalRuleTemplate will create a new CodeCommit Approval Rule Template.
func createCodeCommitApprovalRuleTemplate(t *testing.T, svc *codecommit.CodeCommit) *string {
	uniqueID := util.UniqueID()
	name := fmt.Sprintf("cloud-nuke-testing-%s", uniqueID)

	approvalRuleTemplateContent := "{\"Version\":\"2018-11-08\",\"DestinationReferences\":\"*\",\"Statements\":[{\"Type\":\"Approvers\",\"NumberOfApprovalsNeeded\":1,\"ApprovalPoolMembers\":\"*\"}]}"

	_, err := svc.CreateApprovalRuleTemplate(&codecommit.CreateApprovalRuleTemplateInput{
		ApprovalRuleTemplateName:        aws.String(name),
		ApprovalRuleTemplateDescription: aws.String(name),
		ApprovalRuleTemplateContent:     aws.String(approvalRuleTemplateContent),
	})
	require.NoError(t, err)

	// Verify that the template is generated well
	resp, err := svc.GetApprovalRuleTemplate(&codecommit.GetApprovalRuleTemplateInput{
		ApprovalRuleTemplateName: aws.String(name),
	})
	require.NoError(t, err)
	if resp == nil {
		t.Fatalf("Error creating Approval Rule Template %s", name)
	}
	// And an arbitrary sleep to account for eventual consistency
	time.Sleep(15 * time.Second)
	return &name
}

// deleteCodeCommitApprovalRuleTemplateInTest is a function to delete the given CodeCommit Approval Rule Templates.
func deleteCodeCommitApprovalRuleTemplateInTest(t *testing.T, svc *codecommit.CodeCommit, name *string, checkErr bool) {
	input := &codecommit.DeleteApprovalRuleTemplateInput{ApprovalRuleTemplateName: name}
	_, err := svc.DeleteApprovalRuleTemplate(input)
	if checkErr {
		require.NoError(t, err)
	}
}

func assertCodeCommitApprovalRuleTemplatesDeleted(t *testing.T, svc *codecommit.CodeCommit, identifiers []*string) {
	for _, name := range identifiers {
		resp, err := svc.GetApprovalRuleTemplate(&codecommit.GetApprovalRuleTemplateInput{ApprovalRuleTemplateName: name})
		require.ErrorContainsf(t, err, "ApprovalRuleTemplateDoesNotExistException", err.Error())
		if resp.ApprovalRuleTemplate != nil {
			t.Fatalf("Approval Rule Template %s is not deleted", aws.StringValue(name))
		}
	}
}
