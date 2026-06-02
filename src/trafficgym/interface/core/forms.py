from django import forms
from .models import Scenario, Artefact, Experiment, TransformationRequest, TransformationInput
from django.db import transaction
from trafficgym.engine.transformations.registry import get_spec_or_none
from .tasks import derive_from_artefact
from .utils import get_or_create_artefact_from_upload, ArtefactResolution, safe_delay
from typing import Any
import ast
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

        add_artefacts = list(
            scenario.artefacts.filter(
                original_name__iendswith=".add.xml"
            ).order_by("original_name")
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
            sid = str(scenario.id)

            # Reuse an existing netpreview TR if one already ran (or is running) for this
            # net artefact.  Creating a fresh TR would make image_transformation_request
            # return the new PENDING TR for every scenario sharing the same net file,
            # causing their previews to get stuck on "Generating…" while the fast
            # dedup-reuse path completes before the SSE connection is re-established.
            preview_already_exists = TransformationRequest.objects.filter(
                method=preview_spec.key,
                input_bindings__artefact=net_artefact,
            ).exclude(status="FAILED").exists()

            if not preview_already_exists:
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

                preview_id = str(preview_request.id)
                transaction.on_commit(lambda: safe_delay(
                    derive_from_artefact, preview_id, scenario_id=sid,
                    on_broker_error=lambda _: TransformationRequest.objects.filter(
                        id=preview_id, status="PENDING"
                    ).update(status="FAILED"),
                ))

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

            for i, add_artefact in enumerate(add_artefacts):
                TransformationInput.objects.create(
                    transformation_request=inspect_request,
                    artefact=add_artefact,
                    input_name=f"add_xml_{i}",
                )


            logger.info(
                f"Created inspect transform request "
                f"{inspect_request.id} for scenario {scenario.id}"
            )

            inspect_id = str(inspect_request.id)
            transaction.on_commit(lambda: safe_delay(
                derive_from_artefact, inspect_id, scenario_id=sid,
                on_broker_error=lambda _: TransformationRequest.objects.filter(
                    id=inspect_id, status="PENDING"
                ).update(status="FAILED"),
            ))

        return scenario, created, reused


def _extract_experiment_class_name(source: str) -> str:
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        raise forms.ValidationError(f"Python syntax error: {e}")
    names = [
        node.name for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef)
        and any(
            (isinstance(b, ast.Name) and b.id == "Experiment")
            or (isinstance(b, ast.Attribute) and b.attr == "Experiment")
            for b in node.bases
        )
    ]
    if len(names) == 0:
        raise forms.ValidationError("No Experiment subclass found in the uploaded file.")
    if len(names) > 1:
        raise forms.ValidationError(
            f"Multiple Experiment subclasses found: {', '.join(names)}. Use one class per file."
        )
    return names[0]


class ExperimentForm(forms.ModelForm):
    upload_file = forms.FileField(required=True)

    class Meta:
        model = Experiment
        fields: list[str] = []  # name is inferred from the uploaded file

    def clean(self) -> dict[str, Any]:
        cleaned = super().clean()
        f = self.files.get("upload_file")
        if f:
            source = f.read().decode("utf-8", errors="replace")
            f.seek(0)
            cleaned["name"] = _extract_experiment_class_name(source)
        return cleaned

    def save(
        self, commit: bool = True
    ) -> tuple[Experiment, list[Artefact], list[Artefact]]:
        experiment: Experiment = super().save(commit=False)
        experiment.name = self.cleaned_data["name"]

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
