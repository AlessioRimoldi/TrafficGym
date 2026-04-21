from django import forms
from .models import Scenario, Artefact, Experiment, TransformationRequest
from django.core.files import File
import hashlib


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
