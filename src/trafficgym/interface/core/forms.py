from django import forms
from .models import Scenario, Artefact, Experiment, TransformationRequest, TransformationInput
from django.core.files import File
from django.db import transaction
from trafficgym.engine.transformations.registry import get_spec_or_none
from .tasks import derive_from_artefact
import hashlib
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

    class Meta:
        model = Scenario
        exclude = ["artefacts"]

    def save(
        self, commit: bool = True
    ) -> tuple[Scenario, list[Artefact], list[Artefact]]:
        scenario: Scenario = super().save(commit=commit)

        created = []
        reused = []

        for f in self.files.getlist("upload_files"):
            content = f.read()
            f.seek(0)

            sha256 = hashlib.sha256(content).hexdigest()

            artefact = Artefact.objects.filter(sha256=sha256).first()

            if artefact is None:
                artefact = Artefact.objects.create(file=f)  # type: ignore
                created.append(artefact)

            else:
                reused.append(artefact)

            scenario.artefacts.add(artefact)

        net_artefact = (
            scenario.artefacts.filter(
                original_name__iendswith=".net.xml"
            )
            .order_by("original_name")
            .first()
        )

        spec = get_spec_or_none("netpreview")

        if net_artefact is None:
            logger.warning(
                f"No .net.xml artefact found for scenario {scenario.id}"
            )

        elif spec is None:
            logger.error("No spec found for netpreview. Check the artefact transformation method is registered")

        else:
            transform_request = TransformationRequest.objects.create(
                method=spec.key,
                spec_snapshot={
                    "key": spec.key,
                    "inputs": [
                        {
                            "name": i.name,
                            "type": i.type.value,
                            "required": i.required,
                        }
                        for i in spec.inputs
                    ],
                    "outputs": [o.name for o in spec.outputs],
                    "docstring": spec.docstring,
                },
                parameters={},
            )

            TransformationInput.objects.create(
                transformation_request=transform_request,
                artefact=net_artefact,
                input_name="net_xml",
            )

            logger.info(
                f"Created netpreview transform request "
                f"{transform_request.id} for scenario {scenario.id}"
            )

            transaction.on_commit(
                lambda: derive_from_artefact.delay(
                    str(transform_request.id)
                )
            )


        return scenario, created, reused


class ExperimentForm(forms.ModelForm):
    upload_file = forms.FileField(required=True)

    class Meta:
        model = Experiment
        fields = ["name", "version"]

    def save(
        self, commit: bool = True
    ) -> tuple[Experiment, list[Artefact], list[Artefact]]:
        experiment: Experiment = super().save(commit=False)

        f = self.files.get("upload_file")
        if not f:
            raise Exception("Could not find experiment artefact upload.")
        content = f.read()
        f.seek(0)

        created = []
        reused = []

        sha256 = hashlib.sha256(content).hexdigest()

        artefact = Artefact.objects.filter(sha256=sha256).first()

        if artefact is None:
            artefact = Artefact.objects.create(file=f)  # type: ignore
            created.append(artefact)

        else:
            reused.append(artefact)

        experiment.artefact = artefact

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
            content = f.read()
            f.seek(0)

            sha256 = hashlib.sha256(content).hexdigest()

            artefact = Artefact.objects.filter(sha256=sha256).first()

            if artefact is None:
                artefact = Artefact.objects.create(file=File(f))  # type: ignore
                created.append(artefact)

            else:
                reused.append(artefact)

        return created, reused
