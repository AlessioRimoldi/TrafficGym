from django import forms
from .models import Scenario, Artefact
from typing import cast
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
        required=False
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
                content = f.read()
                artefact = Artefact.objects.create(file=f) #type: ignore
                created.append(artefact)

            else:
                reused.append(artefact)

            scenario.artefacts.add(artefact)

        return scenario, created, reused
