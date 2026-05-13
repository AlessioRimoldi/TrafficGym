from django import forms
from .models import Scenario, Artefact, Experiment, TransformationRequest, TransformationInput
from django.db import transaction
from trafficgym.engine.transformations.registry import get_spec_or_none
from .tasks import derive_from_artefact
from .utils import get_or_create_artefact_from_upload, ArtefactResolution
from typing import Any
import logging

logger = logging.getLogger(__name__)


class ScenarioForm(forms.ModelForm):
    upload_files = forms.FileField(
        widget=forms.TextInput(
            attrs={
                "name": "upload_files",
                "type": "File",
                "class": "form-control",
                "multiple": "True",
            }
        ),
        # required=True, # Field is actually required, validation happens in view
        required=False,
    )

    existing_artefacts = forms.ModelMultipleChoiceField(
        queryset=Artefact.objects.all(),
        required=False,
    )

    class Meta:
        model = Scenario
        exclude = ["artefacts"]
    
    def clean(self) -> dict[str, Any]:
        cleaned = super().clean()

        existing = cleaned.get("existing_artefacts")
        uploaded = self.files.getlist("upload_files")

        if not existing and not uploaded:
            self.add_error(
                "existing_artefacts",
                "Select at least one artefact or upload a file."
            )
            self.add_error(
                "upload_files",
                "Select at least one artefact or upload a file."
            )

        return cleaned

    def save(
        self, commit: bool = True
    ) -> tuple[Scenario, list[Artefact], list[Artefact]]:
        scenario: Scenario = super().save(commit=commit)

        created = []
        reused = []
        existing = self.cleaned_data.get("existing_artefacts", [])
        upload_results: list[ArtefactResolution] = []

        for artefact in self.cleaned_data.get("existing_artefacts", []):
            reused.append(artefact)


        for f in self.files.getlist("upload_files"):
            result = get_or_create_artefact_from_upload(f)
            upload_results.append(result)

            if result.created:
                created.append(result.artefact)

            else:
                reused.append(result.artefact)

        scenario.artefacts.add(*existing, *[r.artefact for r in upload_results])

        net_artefact = (
            scenario.artefacts.filter(
                original_name__iendswith=".net.xml"
            )
            .order_by("original_name")
            .first()
        )

        preview_spec = get_spec_or_none("netpreview")
        inspect_spec = get_spec_or_none("inspect")

        if net_artefact is None:
            logger.warning(
                f"No .net.xml artefact found for scenario {scenario.id}"
            )

        elif preview_spec is None:
            logger.error("No spec found for netpreview. Check the artefact transformation method is registered")

        elif inspect_spec is None:
            logger.error("No spec found for inspect. Check the scenario inspect transformation method is registered")

        else:
            preview_request = TransformationRequest.objects.create(
                method=preview_spec.key,
                spec_snapshot={
                    "key": preview_spec.key,
                    "inputs": [
                        {
                            "name": i.name,
                            "type": i.type.value,
                            "required": i.required,
                        }
                        for i in preview_spec.inputs
                    ],
                    "outputs": [o.name for o in preview_spec.outputs],
                    "docstring": preview_spec.docstring,
                },
                parameters={},
            )

            TransformationInput.objects.create(
                transformation_request=preview_request,
                artefact=net_artefact,
                input_name="net_xml",
            )

            logger.info(
                f"Created netpreview transform request "
                f"{preview_request.id} for scenario {scenario.id}"
            )

            transaction.on_commit(
                lambda: derive_from_artefact.delay(
                    str(preview_request.id)
                )
            )

            inspect_request = TransformationRequest.objects.create(
                method=inspect_spec.key,
                spec_snapshot={
                    "key": inspect_spec.key,
                    "inputs": [
                        {
                            "name": i.name,
                            "type": i.type.value,
                            "required": i.required,
                        }
                        for i in inspect_spec.inputs
                    ],
                    "outputs": [o.name for o in inspect_spec.outputs],
                    "docstring": inspect_spec.docstring,
                },
                parameters={},
            )

            TransformationInput.objects.create(
                transformation_request=inspect_request,
                artefact=net_artefact,
                input_name="net_xml"
            )

            # TransformationInput.objects.create(
            #     transformation_reques=inspect_request,
            #     artefact=add_artefact,
            #     input_name="add_xml"
            # )


            logger.info(
                f"Created inspect transform request "
                f"{inspect_request.id} for scenario {scenario.id}"
            )

            transaction.on_commit(
                lambda: derive_from_artefact.delay(
                    str(inspect_request.id)
                )
            )

        return scenario, created, reused


class ExperimentForm(forms.ModelForm):
    upload_file = forms.FileField(required=True)

    class Meta:
        model = Experiment
        fields = ["name"]

    def save(
        self, commit: bool = True
    ) -> tuple[Experiment, list[Artefact], list[Artefact]]:
        experiment: Experiment = super().save(commit=False)

        created = []
        reused = []

        f = self.files.get("upload_file")
        if not f:
            raise Exception("Could not find experiment artefact upload.")

        result = get_or_create_artefact_from_upload(f)

        if result.created:
            created.append(result.artefact)

        else:
            reused.append(result.artefact)

        experiment.artefact = result.artefact

        if commit:
            experiment.save()

        return experiment, created, reused


class ArtefactForm(forms.ModelForm):
    upload_files = forms.FileField(
        widget=forms.TextInput(
            attrs={
                "name": "upload_files",
                "type": "File",
                "class": "form-control",
                "multiple": "True",
            }
        ),
        # required=True, # Field is actually required, validation happens in view
        required=False,
    )

    class Meta:
        model = Artefact
        exclude = ["file"]

    def save(
        self, commit: bool = True
    ) -> tuple[list[Artefact], list[Artefact]]:
        created = []
        reused = []

        for f in self.files.getlist("upload_files"):
            result = get_or_create_artefact_from_upload(f)

            if result.created:
                created.append(result.artefact)

            else:
                reused.append(result.artefact)

        return created, reused
