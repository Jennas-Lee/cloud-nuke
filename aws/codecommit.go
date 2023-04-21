package aws

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/codecommit"

	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/logging"
	"github.com/gruntwork-io/cloud-nuke/telemetry"
	"github.com/gruntwork-io/go-commons/errors"
	commonTelemetry "github.com/gruntwork-io/go-commons/telemetry"

	"github.com/hashicorp/go-multierror"
)

func getAllCodeCommitRepositories(session *session.Session, excludeAfter time.Time, configObj config.Config) ([]*string, error) {
	svc := codecommit.New(session)

	allRepositories := []*string{}
	repositoryNames := []*string{}
	input := &codecommit.ListRepositoriesInput{}

	// Get repository list
	err := svc.ListRepositoriesPages(
		input,
		func(page *codecommit.ListRepositoriesOutput, lastPage bool) bool {
			for _, repository := range page.Repositories {
				repositoryNames = append(repositoryNames, repository.RepositoryName)
			}
			return !lastPage
		},
	)

	// Get repository metadata from repository list
	for _, repositoryName := range repositoryNames {
		repository, err := svc.GetRepository(&codecommit.GetRepositoryInput{
			RepositoryName: repositoryName,
		})
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		if shouldIncludeCodeCommitRepository(repository.RepositoryMetadata, excludeAfter, configObj) {
			allRepositories = append(allRepositories, repositoryName)
		}
	}

	return allRepositories, errors.WithStackTrace(err)
}

func shouldIncludeCodeCommitRepository(repository *codecommit.RepositoryMetadata, excludeAfter time.Time, configObj config.Config) bool {
	if repository == nil {
		return false
	}

	if repository.LastModifiedDate != nil && excludeAfter.Before(*repository.LastModifiedDate) {
		return false
	}

	return config.ShouldInclude(
		aws.StringValue(repository.RepositoryName),
		configObj.CodeCommitRepository.IncludeRule.NamesRegExp,
		configObj.CodeCommitRepository.ExcludeRule.NamesRegExp,
	)
}

// deleteCodeCommitRepository is a helper method that deletes the given codecommit repository.
func deleteCodeCommitRepository(svc *codecommit.CodeCommit, repositoryName *string) error {
	input := &codecommit.DeleteRepositoryInput{
		RepositoryName: repositoryName,
	}

	_, err := svc.DeleteRepository(input)
	if err != nil {
		return errors.WithStackTrace(err)
	}

	return nil
}

func nukeAllCodeCommitRepositories(session *session.Session, identifiers []*string) error {
	region := aws.StringValue(session.Config.Region)

	svc := codecommit.New(session)

	if len(identifiers) == 0 {
		logging.Logger.Debugf("No CodeCommit Repositories to nuke in region %s", region)
		return nil
	}

	// NOTE: we don't need to do pagination here, because the pagination is handled by the caller to this function,
	// based on CodeCommitRepository.MaxBatchSize, however we add a guard here to warn users when the batching fails and
	// has a chance of throttling AWS. Since we concurrently make one call for each identifier, we pick 100 for the
	// limit here because many APIs in AWS have a limit of 100 requests per second.
	if len(identifiers) > 100 {
		logging.Logger.Errorf("Nuking too many CodeCommit Repositories at once (100): halting to avoid hitting AWS API rate limiting")
		return TooManyCodeCommitRepositoriesErr{}
	}

	logging.Logger.Debugf("Deleting CodeCommit Repositories in region %s", region)

	var multiErr *multierror.Error
	for _, repositoryName := range identifiers {
		if err := deleteCodeCommitRepository(svc, repositoryName); err != nil {
			telemetry.TrackEvent(commonTelemetry.EventContext{
				EventName: "Error Nuking CodeCommit Repository",
			}, map[string]interface{}{
				"region": region,
			})
			logging.Logger.Errorf("[Failed] %s", err)
			multiErr = multierror.Append(multiErr, err)
		} else {
			logging.Logger.Infof("[OK] CodeCommit Repository %s was deleted in %s", aws.StringValue(repositoryName), region)
		}
	}

	return multiErr.ErrorOrNil()
}

func getAllCodeCommitApprovalRuleTemplates(session *session.Session, excludeAfter time.Time, configObj config.Config) ([]*string, error) {
	svc := codecommit.New(session)

	allTemplates := []*string{}
	templateNames := []*string{}
	input := &codecommit.ListApprovalRuleTemplatesInput{}

	// Get repository list
	err := svc.ListApprovalRuleTemplatesPages(
		input,
		func(page *codecommit.ListApprovalRuleTemplatesOutput, lastPage bool) bool {
			for _, templateName := range page.ApprovalRuleTemplateNames {
				templateNames = append(templateNames, templateName)
			}
			return !lastPage
		},
	)

	// Get repository metadata from repository list
	for _, templateName := range templateNames {
		template, err := svc.GetApprovalRuleTemplate(&codecommit.GetApprovalRuleTemplateInput{
			ApprovalRuleTemplateName: templateName,
		})
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		if shouldIncludeCodeCommitApprovalRuleTemplate(template.ApprovalRuleTemplate, excludeAfter, configObj) {
			allTemplates = append(allTemplates, templateName)
		}
	}

	return allTemplates, errors.WithStackTrace(err)
}

func shouldIncludeCodeCommitApprovalRuleTemplate(template *codecommit.ApprovalRuleTemplate, excludeAfter time.Time, configObj config.Config) bool {
	if template == nil {
		return false
	}

	if template.LastModifiedDate != nil && excludeAfter.Before(*template.LastModifiedDate) {
		return false
	}

	return config.ShouldInclude(
		aws.StringValue(template.ApprovalRuleTemplateName),
		configObj.CodeCommitApprovalRuleTemplate.IncludeRule.NamesRegExp,
		configObj.CodeCommitApprovalRuleTemplate.ExcludeRule.NamesRegExp,
	)
}

// deleteCodeCommitApprovalRuleTemplate is a helper method that deletes the given codecommit approval rule template.
func deleteCodeCommitApprovalRuleTemplate(svc *codecommit.CodeCommit, templateName *string) error {
	input := &codecommit.DeleteApprovalRuleTemplateInput{
		ApprovalRuleTemplateName: templateName,
	}

	_, err := svc.DeleteApprovalRuleTemplate(input)
	if err != nil {
		return errors.WithStackTrace(err)
	}

	return nil
}

func nukeAllCodeCommitApprovalRuleTemplates(session *session.Session, identifiers []*string) error {
	region := aws.StringValue(session.Config.Region)

	svc := codecommit.New(session)

	if len(identifiers) == 0 {
		logging.Logger.Debugf("No CodeCommit Approval Rule Templates to nuke in region %s", region)
		return nil
	}

	// NOTE: we don't need to do pagination here, because the pagination is handled by the caller to this function,
	// based on CodeCommitApprovalRuleTemplates.MaxBatchSize, however we add a guard here to warn users when the
	// batching fails and has a chance of throttling AWS. Since we concurrently make one call for each identifier, we
	// pick 100 for the limit here because many APIs in AWS have a limit of 100 requests per second.
	if len(identifiers) > 100 {
		logging.Logger.Errorf("Nuking too many CodeCommit Approval Rule Templates at once (100): halting to avoid hitting AWS API rate limiting")
		return TooManyCodeCommitApprovalRuleTemplates{}
	}

	logging.Logger.Debugf("Deleting CodeCommit Approval Rule Templates in region %s", region)

	var multiErr *multierror.Error
	for _, templateName := range identifiers {
		if err := deleteCodeCommitApprovalRuleTemplate(svc, templateName); err != nil {
			telemetry.TrackEvent(commonTelemetry.EventContext{
				EventName: "Error Nuking CodeCommit Approval Rule Templates",
			}, map[string]interface{}{
				"region": region,
			})
			logging.Logger.Errorf("[Failed] %s", err)
			multiErr = multierror.Append(multiErr, err)
		} else {
			logging.Logger.Infof("[OK] CodeCommit Approval Rule Template %s was deleted in %s", aws.StringValue(templateName), region)
		}
	}

	return multiErr.ErrorOrNil()
}

// Custom errors

type TooManyCodeCommitRepositoriesErr struct{}

type TooManyCodeCommitApprovalRuleTemplates struct{}

func (err TooManyCodeCommitRepositoriesErr) Error() string {
	return "Too many CodeCommit Repositories requested at once."
}

func (err TooManyCodeCommitApprovalRuleTemplates) Error() string {
	return "Too many CodeCommit Approval Rule Templates requested at once."
}
